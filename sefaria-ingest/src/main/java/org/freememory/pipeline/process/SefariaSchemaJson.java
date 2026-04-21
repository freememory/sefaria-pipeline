package org.freememory.pipeline.process;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.List;

/**
 * Jackson model for a Sefaria schema file (data/schemas/{Title}.json).
 *
 * Schema files contain the structural metadata for each text — how it is
 * organized, how it is cited, and what alternative structural divisions exist.
 *
 * === Key Fields ===
 *
 * schema.depth
 *   How many levels deep the text array is (1, 2, or 3).
 *   Bible and Talmud: depth=2 (Chapter/Verse or Daf/Line)
 *   Mishnah: depth=2 (Chapter/Paragraph)
 *   Prose texts: depth=1 (flat list of paragraphs)
 *
 * schema.addressTypes
 *   How each level is addressed. Key values:
 *     "Integer" — simple 1-based numbers (most texts)
 *     "Talmud"  — daf notation (2a, 2b, 3a...) — used for first level of Talmud texts
 *     "Perek"   — Mishnah chapter notation
 *
 * schema.sectionNames
 *   Human-readable names: ["Chapter","Verse"], ["Daf","Line"], ["Halakha"], etc.
 *
 * alts.Parasha
 *   Present only for Torah books (Genesis–Deuteronomy). Contains the 54 weekly
 *   Torah portions, each subdivided into 7 aliyot with verse-range refs like
 *   "Genesis 1:1-1:5". We use these as chunk boundaries for Torah texts.
 *
 * === Commentary Detection ===
 *
 * Commentary texts (Rashi, Ramban, etc.) have category paths containing
 * "Rishonim", "Acharonim", or "Commentary". These are flagged so the pipeline
 * can set is_primary=false in the chunk metadata.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class SefariaSchemaJson
{
    private String title;
    private List<String> categories;
    private JsonNode schema;

    @JsonProperty("alts")
    private JsonNode altStructs;

    @JsonProperty("enDesc")
    private String enDesc;

    @JsonProperty("heDesc")
    private String heDesc;

    // Override fields — used when no schema file is available.
    // Set via setDepthOverride() / setAddressTypesOverride() in TextFileProcessor.
    private int depthOverride = -1;
    private List<String> addressTypesOverride = null;

    // ------------------------------------------------------------------
    // Setters (used to populate fallback instances when no schema file exists)
    // ------------------------------------------------------------------

    public void setTitle(String title)
    {
        this.title = title;
    }

    public void setCategories(List<String> categories)
    {
        this.categories = new ArrayList<>(categories);
    }

    public void setDepthOverride(int depth)
    {
        this.depthOverride = depth;
    }

    public void setAddressTypesOverride(List<String> types)
    {
        this.addressTypesOverride = new ArrayList<>(types);
    }

    // ------------------------------------------------------------------
    // Schema field accessors (delegated into the nested "schema" object)
    // ------------------------------------------------------------------

    /** Depth of the text array (typically 1, 2, or 3). Defaults to 2. */
    @JsonIgnore
    public int getDepth()
    {
        if (depthOverride >= 1)
        {
            return depthOverride;
        }
        if (schema == null)
        {
            return 2;
        }
        JsonNode depthNode = schema.get("depth");
        return depthNode != null ? depthNode.asInt(2) : 2;
    }

    /** Address type names for each depth level. */
    @JsonIgnore
    public List<String> getAddressTypes()
    {
        if (addressTypesOverride != null)
        {
            return addressTypesOverride;
        }
        return getStringList(schema, "addressTypes");
    }

    /** Human-readable section names for each depth level. */
    @JsonIgnore
    public List<String> getSectionNames()
    {
        return getStringList(schema, "sectionNames");
    }

    /**
     * Get the flat list of all aliyah verse-range refs from alt_structs.Parasha.
     *
     * Returns a list like:
     *   ["Genesis 1:1-1:5", "Genesis 1:6-1:8", ..., "Deuteronomy 33:27-34:12"]
     *
     * There are 54 parashiyot × 7 aliyot = 378 entries for the five Torah books.
     * Returns an empty list if this text has no aliyah structure.
     */
    public List<String> getAliyahRefs()
    {
        List<String> refs = new ArrayList<>();
        if (altStructs == null)
        {
            return refs;
        }

        JsonNode parashaNode = altStructs.get("Parasha");
        if (parashaNode == null)
        {
            return refs;
        }

        JsonNode nodes = parashaNode.get("nodes");
        if (nodes == null || !nodes.isArray())
        {
            return refs;
        }

        for (JsonNode parasha : nodes)
        {
            JsonNode aliyotRefs = parasha.get("refs");
            if (aliyotRefs != null && aliyotRefs.isArray())
            {
                for (JsonNode aliyahRef : aliyotRefs)
                {
                    String refStr = aliyahRef.asText();
                    if (!refStr.isBlank())
                    {
                        refs.add(refStr);
                    }
                }
            }
        }
        return refs;
    }

    /** True if this text has Torah aliyah divisions in alt_structs. */
    public boolean hasAliyahStructure()
    {
        return altStructs != null && altStructs.has("Parasha");
    }

    // ------------------------------------------------------------------
    // Getters
    // ------------------------------------------------------------------

    public String getTitle() { return title; }

    public List<String> getCategories()
    {
        return categories != null ? categories : List.of();
    }

    @JsonIgnore
    public String getTopCategory()
    {
        return categories != null && !categories.isEmpty() ? categories.get(0) : "";
    }

    public String getEnDesc() { return enDesc; }

    @JsonIgnore
    public boolean isCommentary()
    {
        if (categories == null)
        {
            return false;
        }
        for (String cat : categories)
        {
            if (cat.contains("Rishonim") || cat.contains("Acharonim")
                    || cat.contains("Commentary") || cat.contains("Modern Commentary"))
            {
                return true;
            }
        }
        return false;
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    private List<String> getStringList(JsonNode parent, String fieldName)
    {
        if (parent == null)
        {
            return List.of();
        }
        JsonNode node = parent.get(fieldName);
        if (node == null || !node.isArray())
        {
            return List.of();
        }
        List<String> result = new ArrayList<>();
        for (JsonNode item : node)
        {
            result.add(item.asText());
        }
        return result;
    }
}
