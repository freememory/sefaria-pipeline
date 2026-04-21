package org.freememory.pipeline.agent.tools;

import dev.langchain4j.agent.tool.P;
import dev.langchain4j.agent.tool.Tool;
import dev.langchain4j.data.embedding.Embedding;
import dev.langchain4j.model.embedding.EmbeddingModel;
import io.qdrant.client.QdrantClient;
import io.qdrant.client.grpc.Common.Condition;
import io.qdrant.client.grpc.Common.FieldCondition;
import io.qdrant.client.grpc.Common.Filter;
import io.qdrant.client.grpc.Common.Match;
import io.qdrant.client.grpc.Points.ScoredPoint;
import io.qdrant.client.grpc.Points.ScrollPoints;
import io.qdrant.client.grpc.Points.ScrollResponse;
import io.qdrant.client.grpc.Points.SearchPoints;
import io.qdrant.client.grpc.Points.WithPayloadSelector;
import io.qdrant.client.grpc.JsonWithInt.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Direct Sefaria text lookup by canonical ref string.
 *
 * Unlike semantic search (which finds thematically related passages), this tool
 * retrieves the exact Sefaria passage identified by a known reference such as
 * "Shabbat 2a" or "Mishneh Torah Laws of Prayer 2:4".
 *
 * This is particularly useful when:
 *   - The user asks "What does Rashi say on Genesis 1:1?"  (known citation)
 *   - The agent retrieved a chunk whose {@code linked_refs} field points to a
 *     related passage that should also be fetched for context
 *   - A previous answer cited a source and the user asks for more detail on it
 *
 * === Implementation ===
 *
 * Uses the Qdrant Scroll API with a keyword filter on the {@code ref} payload
 * field.  The first matching point (highest score is arbitrary for exact match)
 * is returned as formatted text.
 *
 * Note: refs are stored exactly as chunked, so very long passages span multiple
 * chunks.  A ref like "Genesis 1" will match multiple verse-group chunks.
 * This tool returns only the first match; extend it to return all matches if
 * needed.
 */
public class SefariaLookupTools
{
    private static final Logger log = LoggerFactory.getLogger(SefariaLookupTools.class);

    /** Number of results returned by SEFARIA_SEARCH per call. */
    private static final int SEARCH_TOP_K = 8;

    private final QdrantClient   qdrant;
    private final EmbeddingModel embeddingModel;
    private final String         collectionName;

    public SefariaLookupTools(QdrantClient qdrant,
                              EmbeddingModel embeddingModel,
                              String collectionName)
    {
        this.qdrant         = qdrant;
        this.embeddingModel = embeddingModel;
        this.collectionName = collectionName;
    }

    // ------------------------------------------------------------------
    // Tools
    // ------------------------------------------------------------------

    @Tool("""
        Search the Sefaria database for texts related to a topic, concept, or question.
        Use this to retrieve primary sources from the vector database using semantic search.

        IMPORTANT — call this tool multiple times with DIFFERENT search angles for
        comprehensive questions. For example, for "Hallel on Yom HaAtzmaut" you might call:
          1. searchSefaria("Hallel Yom HaAtzmaut opinions")
          2. searchSefaria("Hallel incomplete Rosh Chodesh custom")
          3. searchSefaria("Yom HaAtzmaut Israeli independence halacha responsa")
          4. searchSefaria("contemporary poskim Israeli holidays celebration")

        Each call returns up to 8 chunks from both English and Hebrew sources.
        Combine results from multiple calls for thorough, source-rich answers.

        Use SEFARIA_LOOKUP to fetch the full text of a specific ref found in the results.
        """)
    public String searchSefaria(
            @P("The search query — phrase it as a specific topic, concept, ruling, or question. "
             + "Use different angles across multiple calls to cover the subject comprehensively.")
            String query,
            @P("Optional category filter to restrict results. Use one of: Tanakh, Talmud, "
             + "Mishnah, Halakhah, Responsa, Midrash, Kabbalah, Jewish Thought, Chasidut, "
             + "Liturgy, Musar, Tosefta. Leave empty to search all categories.")
            String category)
    {
        if (query == null || query.isBlank())
        {
            return "Search query must not be empty.";
        }

        // Embed the query
        Embedding queryEmbedding;
        try
        {
            queryEmbedding = embeddingModel.embed(query).content();
        }
        catch (Exception e)
        {
            log.error("Embedding failed for search query '{}': {}", query, e.getMessage());
            return "Failed to embed search query: " + e.getMessage();
        }

        // Build search request with optional category filter
        SearchPoints.Builder search = SearchPoints.newBuilder()
                .setCollectionName(collectionName)
                .addAllVector(toFloatList(queryEmbedding.vector()))
                .setLimit(SEARCH_TOP_K)
                .setWithPayload(WithPayloadSelector.newBuilder().setEnable(true).build());

        if (category != null && !category.isBlank())
        {
            search.setFilter(Filter.newBuilder()
                    .addMust(Condition.newBuilder()
                            .setField(FieldCondition.newBuilder()
                                    .setKey("category")
                                    .setMatch(Match.newBuilder().setKeyword(category).build())
                                    .build())
                            .build())
                    .build());
        }

        List<ScoredPoint> results;
        try
        {
            results = qdrant.searchAsync(search.build()).get();
        }
        catch (Exception e)
        {
            log.error("Qdrant search failed for query '{}': {}", query, e.getMessage());
            return "Search failed: " + e.getMessage();
        }

        if (results.isEmpty())
        {
            return "No results found for: \"" + query + "\""
                    + (category != null && !category.isBlank() ? " in category: " + category : "")
                    + ". Try rephrasing or broadening the search.";
        }

        StringBuilder sb = new StringBuilder();
        sb.append("Search results for: \"").append(query).append("\"");
        if (category != null && !category.isBlank())
        {
            sb.append(" [category: ").append(category).append("]");
        }
        sb.append("\n\n");

        for (ScoredPoint point : results)
        {
            Map<String, Value> payload = point.getPayload();
            String text     = str(payload, "text");
            String ref      = str(payload, "ref");
            String title    = str(payload, "title");
            String lang     = str(payload, "language");
            String cat      = str(payload, "category");

            if (text != null && !text.isBlank())
            {
                sb.append(String.format("[%s | %s | %s | %s | score=%.3f]\n%s\n\n",
                        ref, title, cat, lang, point.getScore(), text));
            }
        }

        return sb.toString().strip();
    }

    @Tool("Retrieve the exact text of a Sefaria passage by its canonical reference. "
        + "Use this when you know the specific citation (e.g. 'Shabbat 2a', "
        + "'Genesis 1:1', 'Mishneh Torah Laws of Prayer 2:4', "
        + "'Rashi on Genesis 1:1'). Returns the text and metadata.")
    public String lookupByRef(
            @P("The canonical Sefaria reference string. Use the standard format: "
             + "'BookName Chapter:Verse' or 'Tractate DafSide' for Talmud. "
             + "For commentaries: 'Rashi on BookName Chapter:Verse'.")
            String ref)
    {
        Filter filter = Filter.newBuilder()
                .addMust(Condition.newBuilder()
                        .setField(FieldCondition.newBuilder()
                                .setKey("ref")
                                .setMatch(Match.newBuilder().setKeyword(ref).build())
                                .build())
                        .build())
                .build();

        ScrollPoints scrollRequest = ScrollPoints.newBuilder()
                .setCollectionName(collectionName)
                .setFilter(filter)
                .setLimit(3)   // return up to 3 matching chunks (covers both en+he)
                .setWithPayload(WithPayloadSelector.newBuilder().setEnable(true).build())
                .build();

        try
        {
            ScrollResponse response = qdrant.scrollAsync(scrollRequest).get();

            if (response.getResultList().isEmpty())
            {
                return "No text found for ref: \"" + ref + "\". "
                        + "Check that the reference format matches Sefaria conventions.";
            }

            StringBuilder sb = new StringBuilder();
            for (var point : response.getResultList())
            {
                Map<String, Value> payload = point.getPayload();
                String text     = str(payload, "text");
                String foundRef = str(payload, "ref");
                String title    = str(payload, "title");
                String lang     = str(payload, "language");

                if (text != null && !text.isBlank())
                {
                    sb.append(String.format("[%s | %s | %s]\n%s\n\n",
                            foundRef, title, lang, text));
                }
            }

            return sb.isEmpty()
                    ? "Found entry for \"" + ref + "\" but it has no text."
                    : sb.toString().strip();
        }
        catch (Exception e)
        {
            log.error("Qdrant lookup failed for ref '{}': {}", ref, e.getMessage());
            return "Error looking up ref \"" + ref + "\": " + e.getMessage();
        }
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    private static String str(Map<String, Value> payload, String key)
    {
        Value v = payload.get(key);
        return (v != null && v.hasStringValue()) ? v.getStringValue() : null;
    }

    private static List<Float> toFloatList(float[] array)
    {
        List<Float> list = new ArrayList<>(array.length);
        for (float f : array) list.add(f);
        return list;
    }
}
