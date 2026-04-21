package org.freememory.pipeline.download;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.freememory.config.PipelineConfig.DownloadConfig.Priority;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Parses and queries the Sefaria books.json catalog.
 *
 * books.json is the master index of all 19,643 texts in the public GCS bucket
 * (gs://sefaria-export). It is maintained by the Sefaria-Export repository and
 * updated monthly. This class loads it from local disk (preferred — the
 * Sefaria-Export repo is already cloned at ../Sefaria-Export/) and provides
 * filtered views for selecting which texts to download.
 *
 * The catalog is loaded synchronously at startup; it is a 20MB JSON file
 * that fits comfortably in memory.
 *
 * Filtering approach:
 *   - Always filter to versionTitle="merged" (most complete combined version)
 *   - Always filter to language in {English, Hebrew}
 *   - Then filter by priority tier (P0=core texts, P3=commentaries)
 */
public class BooksJsonCatalog
{
    private static final Logger log = LoggerFactory.getLogger(BooksJsonCatalog.class);

    private final List<BookEntry> books;
    private final List<SpecialFile> specialFiles;

    // ------------------------------------------------------------------
    // Construction
    // ------------------------------------------------------------------

    private BooksJsonCatalog(CatalogRoot root)
    {
        this.books = root.books != null ? root.books : List.of();
        this.specialFiles = root.specialFiles != null ? root.specialFiles : List.of();
        log.info("Loaded catalog: {} book entries", books.size());
    }

    /** Load from a local books.json file. */
    public static BooksJsonCatalog fromLocalFile(Path booksJsonPath) throws IOException
    {
        ObjectMapper mapper = new ObjectMapper();
        CatalogRoot root = mapper.readValue(booksJsonPath.toFile(), CatalogRoot.class);
        return new BooksJsonCatalog(root);
    }

    // ------------------------------------------------------------------
    // Filtering API
    // ------------------------------------------------------------------

    /** All entries with versionTitle="merged" and language in {English, Hebrew}. */
    public List<BookEntry> getMergedEntries()
    {
        return books.stream()
                .filter(b -> "merged".equalsIgnoreCase(b.getVersionTitle()))
                .filter(b -> "English".equalsIgnoreCase(b.getLanguage())
                          || "Hebrew".equalsIgnoreCase(b.getLanguage()))
                .collect(Collectors.toList());
    }

    /** Merged entries whose top-level category is in the given set. */
    public List<BookEntry> getByTopCategories(Set<String> topCategories)
    {
        return getMergedEntries().stream()
                .filter(b -> topCategories.contains(b.getTopCategory()))
                .collect(Collectors.toList());
    }

    /** All merged entries for a given priority tier. */
    public List<BookEntry> getByPriority(Priority priority)
    {
        Set<String> cats = priority.categories();
        return getMergedEntries().stream()
                .filter(b -> cats.stream().anyMatch(cat -> matchesCategory(b, cat)))
                .collect(Collectors.toList());
    }

    /** All merged entries across all priority tiers, sorted P0 → P3. */
    public List<BookEntry> getAllPrioritized()
    {
        return getMergedEntries().stream()
                .sorted((a, b) -> Integer.compare(priorityOf(a), priorityOf(b)))
                .collect(Collectors.toList());
    }

    /** All special files (links CSVs, table_of_contents.json, etc.). */
    public List<SpecialFile> getSpecialFiles() { return specialFiles; }

    /** Only the links CSV special files. */
    public List<SpecialFile> getLinkFiles()
    {
        return specialFiles.stream()
                .filter(f -> f.getPath() != null && f.getPath().startsWith("links/"))
                .collect(Collectors.toList());
    }

    public int size() { return books.size(); }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    private boolean matchesCategory(BookEntry b, String cat)
    {
        if (b.getCategories() == null)
        {
            return false;
        }
        if (cat.contains("/"))
        {
            // "Tanakh/Torah" matches category path starting with ["Tanakh","Torah"]
            String[] parts = cat.split("/");
            return b.getCategories().size() >= parts.length &&
                   b.getCategories().subList(0, parts.length).equals(List.of(parts));
        }
        return b.getTopCategory().equals(cat);
    }

    private int priorityOf(BookEntry b)
    {
        for (Priority p : Priority.values())
        {
            if (p.categories().stream().anyMatch(cat -> matchesCategory(b, cat)))
            {
                return p.ordinal();
            }
        }
        return Priority.values().length;
    }

    // ------------------------------------------------------------------
    // Jackson inner models
    // ------------------------------------------------------------------

    @JsonIgnoreProperties(ignoreUnknown = true)
    static class CatalogRoot
    {
        @JsonProperty("books")
        List<BookEntry> books;
        @JsonProperty("special_files")
        List<SpecialFile> specialFiles;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class SpecialFile
    {
        private String path;
        private String url;
        private Long size;

        public String getPath() { return path; }
        public String getUrl() { return url; }
        public Long getSize() { return size; }
    }
}
