package org.freememory.pipeline.process;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Processes a single downloaded Sefaria text JSON file into a list of TextChunks.
 *
 * === Pipeline for one file ===
 *
 *   1. Parse merged.json → SefariaTextJson (title, language, text array)
 *   2. Load corresponding schema → SefariaSchemaJson (depth, addressTypes, alt_structs)
 *   3. Select HTML cleaning strategy based on category
 *   4. Walk the text array → list of (text, indices, ref) Segments
 *   5. Chunk the segments according to the text type's strategy
 *   6. (Caller responsibility) Enrich chunks with linked refs via ChunkEnricher
 *
 * === Schema fallback ===
 *
 * If the schema file is not found (download may have failed), a default schema
 * is assumed: depth=2, Integer/Integer addressing (works for most texts).
 * A warning is logged.
 *
 * === Jackson deserialization of the text array ===
 *
 * The "text" field is a JsonNode (arbitrary nested array). We convert it to
 * a plain Java List recursively before passing to TextWalker, since TextWalker
 * operates on plain Java objects.
 */
public class TextFileProcessor
{
    private static final Logger log = LoggerFactory.getLogger(TextFileProcessor.class);

    private final ObjectMapper mapper = new ObjectMapper();
    private final Chunker chunker = new Chunker();

    /**
     * Process one text JSON file into chunks.
     *
     * @param textFile    path to the downloaded merged.json file
     * @param schemaFile  path to the corresponding schema JSON (may be null)
     * @param categories  the category path (used to build metadata; from BookEntry)
     * @return list of chunks ready for enrichment and embedding
     */
    public List<TextChunk> process(Path textFile, Path schemaFile, List<String> categories)
            throws Exception
    {
        // Step 1: Parse text JSON
        SefariaTextJson textJson = mapper.readValue(textFile.toFile(), SefariaTextJson.class);

        if (textJson.getText() == null)
        {
            log.warn("No text content in {}", textFile);
            return List.of();
        }

        // Step 2: Load schema (or use defaults)
        SefariaSchemaJson schema = loadSchema(schemaFile, textJson.getTitle(), categories);

        // Step 3: Determine HTML cleaning strategy
        HtmlCleaner.FootnoteStrategy footnoteStrategy =
                HtmlCleaner.strategyFor(schema.getTopCategory());

        // Step 4: Walk text array
        TextWalker walker = new TextWalker(
                textJson.getTitle(),
                schema.getDepth(),
                schema.getAddressTypes(),
                footnoteStrategy);

        Object textObj = jsonNodeToObject(textJson.getText());
        List<TextWalker.Segment> segments = walker.walk(textObj);

        if (segments.isEmpty())
        {
            log.debug("No segments found in {}", textFile);
            return List.of();
        }

        // Step 5: Build chunk template from metadata
        TextChunk template = buildTemplate(textJson, schema, categories);

        // Step 6: Chunk
        List<String> aliyahRefs = schema.getAliyahRefs();
        return chunker.chunk(segments, schema, aliyahRefs, template);
    }

    // ------------------------------------------------------------------
    // Private helpers
    // ------------------------------------------------------------------

    private SefariaSchemaJson loadSchema(Path schemaFile, String title, List<String> categories)
    {
        if (schemaFile != null && Files.exists(schemaFile))
        {
            try
            {
                return mapper.readValue(schemaFile.toFile(), SefariaSchemaJson.class);
            }
            catch (Exception e)
            {
                log.warn("Failed to parse schema for {}: {}", title, e.getMessage());
            }
        }
        else
        {
            log.debug("No schema file for {} ({}); using category-inferred defaults",
                    title, categories.isEmpty() ? "unknown" : categories.get(0));
        }

        // Fallback schema — category and title are known from the file path even
        // when the GCS schema file wasn't found. Infer depth and addressTypes from
        // the top-level category so the chunker uses the right strategy.
        String topCat = categories.isEmpty() ? "" : categories.get(0);
        SefariaSchemaJson fallback = new SefariaSchemaJson();
        fallback.setTitle(title);
        if (!categories.isEmpty())
        {
            fallback.setCategories(categories);
        }
        fallback.setDepthOverride(inferDepth(topCat));
        fallback.setAddressTypesOverride(inferAddressTypes(topCat));
        return fallback;
    }

    /**
     * Infer the text array depth from the top-level category.
     *
     * Prose genres (Responsa, Jewish Thought, Chasidut, Musar, Kabbalah) are
     * flat lists of paragraphs: depth=1. Everything else is verse/segment
     * structured: depth=2. Commentary texts can be depth=2 or 3; we default
     * to 2 and rely on the walker to stop at actual leaf strings.
     */
    private static int inferDepth(String topCategory)
    {
        switch (topCategory)
        {
            case "Responsa":
            case "Jewish Thought":
            case "Chasidut":
            case "Musar":
                return 1;
            default:
                return 2;
        }
    }

    /**
     * Infer addressTypes from the top-level category.
     *
     * Only Talmud needs special treatment (daf notation). Tanakh uses "Perek"
     * and "Pasuk" in the schema files but those are functionally identical to
     * integer addressing for our purposes — we include them explicitly so the
     * fallback mirrors what a real schema would contain.
     */
    private static java.util.List<String> inferAddressTypes(String topCategory)
    {
        switch (topCategory)
        {
            case "Talmud":
                return java.util.List.of("Talmud", "Integer");
            case "Tanakh":
                return java.util.List.of("Perek", "Pasuk");
            default:
                return java.util.List.of("Integer", "Integer");
        }
    }

    private TextChunk buildTemplate(SefariaTextJson text,
                                     SefariaSchemaJson schema,
                                     List<String> categories)
    {
        TextChunk t = new TextChunk();
        t.setTitle(text.getTitle());
        t.setLanguage(text.getLanguageCode());
        t.setVersionTitle("merged");
        t.setCategory(categories.isEmpty() ? schema.getTopCategory() : categories.get(0));
        t.setSubcategory(categories.size() > 1 ? categories.get(1) : "");
        t.setFullCategories(categories.isEmpty() ? schema.getCategories() : categories);
        t.setPrimary(!schema.isCommentary());
        return t;
    }

    /**
     * Convert a Jackson JsonNode to a plain Java object tree that TextWalker
     * can traverse: {@code List<Object>} for arrays/objects, {@code String}
     * for text leaves.
     *
     * === Object nodes ===
     *
     * Some Sefaria texts (e.g. Chayei Moharan, many Chasidut and Kabbalah
     * works) have a complex schema where the top-level "text" field is a JSON
     * *object* with named sections rather than a plain array:
     *
     *   "text": {
     *       "Introduction": [],
     *       "": [[ "passage 1", "passage 2", ... ]]
     *   }
     *
     * We treat object nodes the same as array nodes: collect the field values
     * (in document order, ignoring keys) into a List. The section names carry
     * no information the walker or chunker uses.
     */
    private Object jsonNodeToObject(JsonNode node)
    {
        if (node == null || node.isNull())
        {
            return null;
        }
        if (node.isTextual())
        {
            return node.asText();
        }
        if (node.isArray())
        {
            List<Object> list = new ArrayList<>();
            for (JsonNode child : node)
            {
                list.add(jsonNodeToObject(child));
            }
            return list;
        }
        if (node.isObject())
        {
            // Named-section format: collect the field values in document order.
            List<Object> list = new ArrayList<>();
            for (JsonNode child : node) // Jackson iterates over values, not keys
            {
                list.add(jsonNodeToObject(child));
            }
            return list;
        }
        // Numeric / boolean leaf — convert to string
        return node.asText();
    }
}
