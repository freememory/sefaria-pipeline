package org.freememory.pipeline.process;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.TextNode;

/**
 * Cleans HTML markup embedded in Sefaria text segments.
 *
 * Sefaria's JSON exports embed HTML directly in the text strings:
 *   - &lt;b&gt;/&lt;strong&gt; — emphasis (Talmud Mishnah headers, key terms)
 *   - &lt;em&gt;/&lt;i&gt;  — italics (Aramaic terms, book titles)
 *   - &lt;sup&gt;           — footnotes (translation notes or textual annotations)
 *   - &lt;br&gt;            — line breaks within a verse or paragraph
 *   - &lt;span&gt;          — various formatting and commentary markers
 *
 * The cleaning strategy differs by text type:
 *
 *   DROP    (Bible, Mishnah, Halacha):
 *     Footnotes (&lt;sup&gt;) are translator's notes and add noise. Drop them.
 *
 *   BRACKET (Talmud):
 *     Footnotes clarify Aramaic terms and are semantically valuable.
 *     Wrap them in [square brackets] to preserve the content while marking
 *     it as an annotation rather than part of the primary text.
 */
public class HtmlCleaner
{
    public enum FootnoteStrategy
    {
        DROP,
        BRACKET
    }

    /**
     * Clean HTML from a text segment.
     *
     * @param html             raw string (may or may not contain HTML)
     * @param footnoteStrategy how to handle &lt;sup&gt; footnote content
     * @return plain text with all HTML removed and whitespace normalized
     */
    public static String clean(String html, FootnoteStrategy footnoteStrategy)
    {
        if (html == null || html.isBlank())
        {
            return "";
        }

        Document doc = Jsoup.parseBodyFragment(html);

        for (Element sup : doc.select("sup"))
        {
            if (footnoteStrategy == FootnoteStrategy.BRACKET)
            {
                sup.replaceWith(new TextNode(" [" + sup.text() + "]"));
            }
            else
            {
                sup.remove();
            }
        }

        // Preserve line breaks as spaces
        for (Element br : doc.select("br"))
        {
            br.replaceWith(new TextNode(" "));
        }

        String text = doc.body().text();
        return text.replaceAll("\\s+", " ").trim();
    }

    /** Convenience: clean for Bible/Mishnah (drop footnotes). */
    public static String cleanPrimary(String html)
    {
        return clean(html, FootnoteStrategy.DROP);
    }

    /** Convenience: clean for Talmud (preserve footnotes in brackets). */
    public static String cleanTalmud(String html)
    {
        return clean(html, FootnoteStrategy.BRACKET);
    }

    /** Select the appropriate footnote strategy based on top-level category. */
    public static FootnoteStrategy strategyFor(String topCategory)
    {
        if ("Talmud".equals(topCategory))
        {
            return FootnoteStrategy.BRACKET;
        }
        return FootnoteStrategy.DROP;
    }
}
