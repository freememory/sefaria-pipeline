package org.freememory.pipeline.process;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;

/**
 * A single chunk of text ready for embedding and ingestion into the vector database.
 *
 * === What is a chunk? ===
 *
 * A chunk is a semantically coherent passage from a Sefaria text, sized to
 * fit within the embedding model's context window (~300–450 tokens). Examples:
 *   - One aliyah of the Torah (e.g., Genesis 1:1–1:5)
 *   - One amud (page-side) of Talmud (e.g., Shabbat 2a)
 *   - One numbered Mishnah paragraph (e.g., Mishnah Berakhot 1:1)
 *   - One halacha ruling (e.g., Mishneh Torah, Prayer 2:4)
 *   - One prose paragraph (Responsa, Jewish Thought)
 *
 * === Bilingual design ===
 *
 * Hebrew and English versions of the same passage are stored as *separate*
 * chunks, linked only by their matching `ref` field. This keeps each chunk
 * monolingual, which produces cleaner embeddings. At query time, the agent
 * uses a language filter or ref-based lookup to retrieve the paired version.
 *
 * === chunk_id ===
 *
 * A SHA-256 hash of (title + ref + language + chunkIndex). This serves as
 * the idempotency key — the pipeline skips chunks already present in Qdrant.
 *
 * === token_estimate ===
 *
 * A rough approximation: wordCount × 1.3. This is consistent with typical
 * English and Hebrew token densities for text-embedding-3-small. Used only
 * for chunking logic; actual token counts are reported by the embedding API.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class TextChunk
{
    // --- Identity ---
    private String chunkId;
    private String ref;
    private String sectionRef;

    // --- Source ---
    private String title;
    private String language;
    private String versionTitle;
    private String chunkStrategy;

    // --- Classification ---
    private String category;
    private String subcategory;
    private List<String> fullCategories;
    private boolean primary;
    private String halakhaTopic;

    // --- Position ---
    private List<Integer> startIndices;
    private int chunkIndex;

    // --- Content ---
    private String text;
    private int wordCount;
    private int tokenEstimate;

    // --- Links (populated by ChunkEnricher) ---
    private List<String> linkedRefs;
    private List<String> linkTypes;

    // --- Commentary only ---
    private String baseRef;

    // ------------------------------------------------------------------
    // Construction
    // ------------------------------------------------------------------

    public TextChunk() {}

    /** Compute and set the chunk_id as SHA-256(title|ref|language|chunkIndex). */
    public void computeChunkId()
    {
        String input = title + "|" + ref + "|" + language + "|" + chunkIndex;
        try
        {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] hash = md.digest(input.getBytes(StandardCharsets.UTF_8));
            this.chunkId = HexFormat.of().formatHex(hash);
        }
        catch (NoSuchAlgorithmException e)
        {
            this.chunkId = Integer.toHexString(input.hashCode());
        }
    }

    // ------------------------------------------------------------------
    // Getters and setters
    // ------------------------------------------------------------------

    public String getChunkId() { return chunkId; }
    public void setChunkId(String chunkId) { this.chunkId = chunkId; }

    public String getRef() { return ref; }
    public void setRef(String ref) { this.ref = ref; }

    public String getSectionRef() { return sectionRef; }
    public void setSectionRef(String sectionRef) { this.sectionRef = sectionRef; }

    public String getTitle() { return title; }
    public void setTitle(String title) { this.title = title; }

    public String getLanguage() { return language; }
    public void setLanguage(String language) { this.language = language; }

    public String getVersionTitle() { return versionTitle; }
    public void setVersionTitle(String versionTitle) { this.versionTitle = versionTitle; }

    public String getChunkStrategy() { return chunkStrategy; }
    public void setChunkStrategy(String chunkStrategy) { this.chunkStrategy = chunkStrategy; }

    public String getCategory() { return category; }
    public void setCategory(String category) { this.category = category; }

    public String getSubcategory() { return subcategory; }
    public void setSubcategory(String subcategory) { this.subcategory = subcategory; }

    public List<String> getFullCategories() { return fullCategories; }
    public void setFullCategories(List<String> fullCategories) { this.fullCategories = fullCategories; }

    public boolean isPrimary() { return primary; }
    public void setPrimary(boolean primary) { this.primary = primary; }

    public String getHalakhaTopic() { return halakhaTopic; }
    public void setHalakhaTopic(String halakhaTopic) { this.halakhaTopic = halakhaTopic; }

    public List<Integer> getStartIndices() { return startIndices; }
    public void setStartIndices(List<Integer> startIndices) { this.startIndices = startIndices; }

    public int getChunkIndex() { return chunkIndex; }
    public void setChunkIndex(int chunkIndex) { this.chunkIndex = chunkIndex; }

    public String getText() { return text; }
    public void setText(String text)
    {
        this.text = text;
        this.wordCount = (text == null || text.isBlank()) ? 0 : text.split("\\s+").length;
        this.tokenEstimate = (int) (wordCount * 1.3);
    }

    public int getWordCount() { return wordCount; }
    public int getTokenEstimate() { return tokenEstimate; }

    public List<String> getLinkedRefs() { return linkedRefs; }
    public void setLinkedRefs(List<String> linkedRefs) { this.linkedRefs = linkedRefs; }

    public List<String> getLinkTypes() { return linkTypes; }
    public void setLinkTypes(List<String> linkTypes) { this.linkTypes = linkTypes; }

    public String getBaseRef() { return baseRef; }
    public void setBaseRef(String baseRef) { this.baseRef = baseRef; }

    @Override
    public String toString()
    {
        return "TextChunk{ref='" + ref + "', lang=" + language +
               ", tokens=" + tokenEstimate + "}";
    }
}
