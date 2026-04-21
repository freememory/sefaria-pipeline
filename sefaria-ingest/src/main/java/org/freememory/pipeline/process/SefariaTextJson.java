package org.freememory.pipeline.process;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.JsonNode;

/**
 * Jackson model for a Sefaria merged text JSON file.
 *
 * Each downloaded file (e.g. data/raw/Tanakh/Torah/Genesis/English/merged.json)
 * has this structure at the top level:
 *
 *   {
 *     "title": "Genesis",
 *     "language": "en",
 *     "versionTitle": "merged",
 *     "versionSource": "https://www.sefaria.org/Genesis",
 *     "text": [ [...], [...], ... ]
 *   }
 *
 * The "text" field is a jagged nested array of strings whose depth is defined by
 * the corresponding schema file (data/schemas/Genesis.json). We deserialize it
 * as a generic JsonNode to handle arbitrary nesting depth, then convert it to a
 * plain Java List structure for the TextWalker.
 *
 * Language codes:
 *   Sefaria uses "en" and "he" in the text file, but the outer books.json uses
 *   "English" and "Hebrew". The getLanguageCode() method normalizes to "en"/"he".
 *   Aramaic texts also use Hebrew script and are treated as "he" for embeddings.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class SefariaTextJson
{
    private String title;
    private String language;
    private String versionTitle;
    private String versionSource;
    private JsonNode text;

    public String getTitle() { return title; }
    public String getLanguage() { return language; }
    public String getVersionTitle() { return versionTitle; }
    public String getVersionSource() { return versionSource; }
    public JsonNode getText() { return text; }

    /**
     * Normalized two-character language code for use in metadata.
     * Hebrew and Aramaic both return "he" since they share the same script
     * and will use the same embedding model.
     */
    public String getLanguageCode()
    {
        if (language == null)
        {
            return "unknown";
        }
        switch (language.toLowerCase())
        {
            case "english": return "en";
            case "hebrew":  return "he";
            case "aramaic": return "he";
            default:
                return language.toLowerCase().substring(0, Math.min(2, language.length()));
        }
    }
}
