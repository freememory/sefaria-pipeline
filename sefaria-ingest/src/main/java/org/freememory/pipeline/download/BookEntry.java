package org.freememory.pipeline.download;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * One entry in Sefaria's books.json — a single text/language/version combination.
 *
 * books.json contains 19,643 entries. Each entry represents one downloadable
 * file in the GCS bucket. The same text (e.g., "Genesis") appears multiple
 * times — once per language and once per version. We always filter for
 * versionTitle = "merged", which is the most complete combined version.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class BookEntry
{
    private String title;
    private String language;
    private String versionTitle;
    private List<String> categories;

    @JsonProperty("json_url")
    private String jsonUrl;

    @JsonProperty("txt_url")
    private String txtUrl;

    // --- Getters ---

    public String getTitle() { return title; }
    public String getLanguage() { return language; }
    public String getVersionTitle() { return versionTitle; }
    public List<String> getCategories() { return categories; }
    public String getJsonUrl() { return jsonUrl; }
    public String getTxtUrl() { return txtUrl; }

    /** Top-level category (e.g. "Tanakh", "Talmud", "Halakhah") */
    public String getTopCategory()
    {
        return (categories != null && !categories.isEmpty()) ? categories.get(0) : "";
    }

    /** Whether this entry is a commentary (Rishonim, Acharonim, etc.) */
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

    /**
     * Local file path relative to the output root directory.
     * Mirrors the GCS layout: {Category1}/{Category2}/.../{Title}/{Language}/merged.json
     */
    public String getRelativePath()
    {
        StringBuilder sb = new StringBuilder();
        if (categories != null)
        {
            for (String cat : categories)
            {
                sb.append(sanitize(cat)).append("/");
            }
        }
        sb.append(sanitize(title)).append("/");
        sb.append(sanitize(language)).append("/");
        sb.append("merged.json");
        return sb.toString();
    }

    private static String sanitize(String s)
    {
        return s.replace(":", "_").replace("\"", "").replace("?", "").replace("*", "");
    }

    @Override
    public String toString()
    {
        return title + " [" + language + "] (" +
               String.join(" > ", categories != null ? categories : List.of()) + ")";
    }
}
