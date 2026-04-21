package org.freememory.pipeline.process;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Recursively traverses a Sefaria jagged text array and emits leaf text segments.
 *
 * === The Jagged Array ===
 *
 * Sefaria texts are stored as nested JSON arrays of strings. The nesting depth
 * varies by text type:
 *
 *   depth=1: ["segment1", "segment2", ...]
 *   depth=2: [["verse1", "verse2"], ["verse1", ...], ...]
 *   depth=3: [[["line1", ...], ...], ...]
 *
 * Some arrays are "jagged" — not all sub-arrays have the same length, and
 * some positions may be null or empty strings (gaps in the text).
 *
 * === Complex-schema texts ===
 *
 * Some texts (many Chasidut, Kabbalah works) have a top-level object with
 * named sections rather than a plain array. After {@code jsonNodeToObject}
 * converts those objects to a List of their values, the walker sees an extra
 * level of nesting compared to what the schema depth field reports.
 *
 * To handle both regular and complex schemas robustly, the walker does NOT
 * use the schema depth as a fixed stop criterion. Instead it walks until it
 * actually encounters String leaves — emitting each one as a segment. This
 * is strictly equivalent for well-formed texts and correctly handles every
 * variant of unexpected extra nesting.
 *
 * The {@code depth} constructor parameter is retained for API compatibility
 * but is no longer used in the recursion.
 *
 * === Output ===
 *
 * For each non-empty leaf string, emits a {@link Segment} containing:
 *   - text:    the cleaned plain text of this segment
 *   - indices: 1-based position at each nesting level
 *   - ref:     the canonical Sefaria Ref string (e.g. "Genesis 3:7")
 *
 * HTML cleaning is applied here, using the footnote strategy appropriate
 * for the text's category (DROP for Bible/Mishnah, BRACKET for Talmud).
 */
public class TextWalker
{
    /** A single leaf-level segment of a text. */
    public static class Segment
    {
        private final String text;
        private final List<Integer> indices;
        private final String ref;

        public Segment(String text, List<Integer> indices, String ref)
        {
            this.text = text;
            this.indices = indices;
            this.ref = ref;
        }

        public String text() { return text; }
        public List<Integer> indices() { return indices; }
        public String ref() { return ref; }

        @Override
        public String toString()
        {
            return ref + ": " + text.substring(0, Math.min(60, text.length()));
        }
    }

    private final String title;
    private final int depth;
    private final List<String> addressTypes;
    private final HtmlCleaner.FootnoteStrategy footnoteStrategy;

    public TextWalker(String title,
                      int depth,
                      List<String> addressTypes,
                      HtmlCleaner.FootnoteStrategy footnoteStrategy)
    {
        this.title = title;
        this.depth = depth;
        this.addressTypes = addressTypes != null ? addressTypes : List.of();
        this.footnoteStrategy = footnoteStrategy;
    }

    /**
     * Walk the text array and return all non-empty leaf segments in document order.
     *
     * @param textArray the "text" value from the Sefaria JSON (a nested List/String)
     */
    public List<Segment> walk(Object textArray)
    {
        List<Segment> segments = new ArrayList<>();
        recurse(textArray, new ArrayList<>(), segments);
        return segments;
    }

    // ------------------------------------------------------------------
    // Private recursion
    // ------------------------------------------------------------------

    private void recurse(Object node, List<Integer> currentIndices, List<Segment> out)
    {
        if (node instanceof String)
        {
            // Leaf string — emit as a segment.
            String raw = (String) node;
            if (!raw.isBlank())
            {
                String clean = HtmlCleaner.clean(raw, footnoteStrategy);
                if (!clean.isBlank())
                {
                    String ref = RefBuilder.buildRef(
                            title,
                            Collections.unmodifiableList(currentIndices),
                            addressTypes);
                    out.add(new Segment(clean, List.copyOf(currentIndices), ref));
                }
            }
        }
        else if (node instanceof List)
        {
            // Intermediate array — recurse into each element with 1-based index.
            List<?> list = (List<?>) node;
            for (int i = 0; i < list.size(); i++)
            {
                List<Integer> childIndices = new ArrayList<>(currentIndices);
                childIndices.add(i + 1);
                recurse(list.get(i), childIndices, out);
            }
        }
        // else: null or unexpected type — silently skip
    }
}
