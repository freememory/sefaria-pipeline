package org.freememory.pipeline.process;

import java.util.List;

/**
 * Builds canonical Sefaria Ref strings from a title and position indices.
 *
 * Sefaria uses a uniform citation system across all text types:
 *
 *   "Genesis 1:3"               — Bible (Chapter:Verse, Integer/Integer addressing)
 *   "Shabbat 2b:5"              — Talmud (Daf:Line, Talmud/Integer addressing)
 *   "Mishnah Berakhot 1:1"      — Mishnah (Chapter:Paragraph)
 *   "Mishneh Torah, Prayer 2:4" — Halacha (Chapter:Section)
 *
 * === Talmud Daf (Page) Addressing ===
 *
 * This is the most complex case. Talmudic texts use a traditional pagination
 * system where:
 *   - Each physical page is called a "daf"
 *   - Each daf has two sides: "amud alef" (side a) and "amud bet" (side b)
 *   - Pagination starts at daf 2 — daf 1 was historically the title page
 *
 * The Sefaria text array uses 1-based integer indices, mapping to daf strings:
 *
 *   Index 1 → "2a"   Index 2 → "2b"
 *   Index 3 → "3a"   Index 4 → "3b"
 *   Index 5 → "4a"   ...
 *
 * Formula:
 *   dafNumber = (index - 1) / 2 + 2   (integer division)
 *   side      = index % 2 == 1 ? "a" : "b"
 */
public class RefBuilder
{
    /**
     * Build a ref for a single position (leaf node in the text array).
     *
     * @param title        canonical text title (e.g., "Genesis", "Shabbat")
     * @param indices      1-based position at each depth level (e.g., [3, 7])
     * @param addressTypes address type names from the schema
     *                     (e.g., ["Integer","Integer"] or ["Talmud","Integer"])
     * @return Sefaria ref string (e.g., "Genesis 3:7" or "Shabbat 3a:7")
     */
    public static String buildRef(String title, List<Integer> indices, List<String> addressTypes)
    {
        if (indices == null || indices.isEmpty())
        {
            return title;
        }

        StringBuilder sb = new StringBuilder(title).append(" ");

        for (int i = 0; i < indices.size(); i++)
        {
            if (i > 0)
            {
                sb.append(":");
            }
            int idx = indices.get(i);
            String addrType = (addressTypes != null && i < addressTypes.size())
                    ? addressTypes.get(i) : "Integer";
            sb.append(formatIndex(idx, addrType));
        }

        return sb.toString();
    }

    /**
     * Build a span ref covering a range of positions.
     * Used for aliyah/chapter chunks: e.g., "Genesis 1:1-1:5" or "Shabbat 2a-2b".
     */
    public static String buildSpanRef(String title,
                                      List<Integer> startIndices,
                                      List<Integer> endIndices,
                                      List<String> addressTypes)
    {
        String startRef = buildRef(title, startIndices, addressTypes);

        StringBuilder endPart = new StringBuilder();
        for (int i = 0; i < endIndices.size(); i++)
        {
            if (i > 0)
            {
                endPart.append(":");
            }
            int idx = endIndices.get(i);
            String addrType = (addressTypes != null && i < addressTypes.size())
                    ? addressTypes.get(i) : "Integer";
            endPart.append(formatIndex(idx, addrType));
        }

        String startLocator = startRef.substring(title.length() + 1);
        if (startLocator.equals(endPart.toString()))
        {
            return startRef;
        }

        return startRef + "-" + endPart;
    }

    /**
     * Build a section-level ref (parent of leaf nodes).
     * e.g., "Genesis 1" as the parent of all verses in chapter 1.
     */
    public static String buildSectionRef(String title, int sectionIndex, String addressType)
    {
        return title + " " + formatIndex(sectionIndex, addressType);
    }

    /**
     * Convert a 1-based Talmud array index to the traditional daf string.
     *
     *   Index 1 → "2a"  (dafNum=(1-1)/2+2=2, odd→"a")
     *   Index 2 → "2b"  (dafNum=(2-1)/2+2=2, even→"b")
     *   Index 3 → "3a"  (dafNum=(3-1)/2+2=3, odd→"a")
     */
    public static String talmudDaf(int index)
    {
        int dafNum = (index - 1) / 2 + 2;
        String side = (index % 2 == 1) ? "a" : "b";
        return dafNum + side;
    }

    // ------------------------------------------------------------------
    // Private
    // ------------------------------------------------------------------

    private static String formatIndex(int idx, String addressType)
    {
        if ("Talmud".equals(addressType))
        {
            return talmudDaf(idx);
        }
        return String.valueOf(idx);
    }
}
