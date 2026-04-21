package org.freememory.pipeline.process;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.regex.Pattern;

/**
 * Splits a Sefaria text into chunks for embedding and vector DB ingestion.
 *
 * === Strategy Selection ===
 *
 * The chunking strategy is chosen based on the text's category and schema:
 *
 *   ALIYAH   — Torah books (Genesis–Deuteronomy).
 *              Uses the aliyah divisions from schema's alt_structs.Parasha.
 *              54 portions × 7 aliyot = ~378 chunks per Torah book.
 *              These are the finest traditional semantic units available.
 *              Note: parashot setumot/petuchot (ס/פ markers) are NOT available
 *              in Sefaria's export — aliyot are the best we can do.
 *
 *   CHAPTER  — Nevi'im and Ketuvim (prophets and writings).
 *              One chunk per chapter. Chapters are natural literary units.
 *
 *   AMUD     — Talmud Bavli and Yerushalmi.
 *              One chunk per amud (page-side: 2a, 2b, 3a, ...).
 *              The amud is the traditional unit of Talmud study.
 *              Includes a 2-line overlap from the next amud for discourse continuity.
 *
 *   PER_UNIT — Mishnah, Tosefta, Mishneh Torah, Shulchan Arukh.
 *              One chunk per numbered paragraph. Each is a self-contained
 *              ruling or statement (a natural semantic unit).
 *
 *   PROSE    — Responsa, Jewish Thought, Midrash, free-form texts.
 *              Sentence-boundary chunking at ~300–400 tokens, with ~50-token overlap.
 *
 * === Token Cap ===
 *
 * All strategies enforce a hard cap of MAX_TOKENS (450) per chunk.
 * If a natural unit (e.g., a long chapter or a verbose halacha ruling) exceeds
 * the cap, it is split at the nearest sentence boundary.
 */
public class Chunker
{
    private static final Logger log = LoggerFactory.getLogger(Chunker.class);

    static final int MAX_TOKENS = 450;
    static final int TARGET_PROSE_TOKENS = 350;
    static final int PROSE_OVERLAP_TOKENS = 50;

    // Fixed-length lookbehind — O(n), no backtracking risk on long texts.
    private static final Pattern SENTENCE_SPLIT =
            Pattern.compile("(?<=[.!?;׃])\\s+");

    private static final Set<String> TORAH_BOOKS = new HashSet<>(Arrays.asList(
            "Genesis", "Exodus", "Leviticus", "Numbers", "Deuteronomy",
            "Bereishit", "Shemot", "Vayikra", "Bamidbar", "Devarim"
    ));

    public enum Strategy
    {
        ALIYAH, CHAPTER, AMUD, PER_UNIT, PROSE
    }

    // ------------------------------------------------------------------
    // Strategy selection
    // ------------------------------------------------------------------

    public static Strategy selectStrategy(SefariaSchemaJson schema)
    {
        String topCat = schema.getTopCategory();
        String title = schema.getTitle();

        switch (topCat)
        {
            case "Tanakh":
                if (TORAH_BOOKS.contains(title) && schema.hasAliyahStructure())
                {
                    return Strategy.ALIYAH;
                }
                return Strategy.CHAPTER;

            case "Talmud":
                return Strategy.AMUD;

            case "Mishnah":
            case "Tosefta":
                return Strategy.PER_UNIT;

            case "Halakhah":
                return schema.getDepth() >= 2 ? Strategy.PER_UNIT : Strategy.PROSE;

            default:
                // PER_UNIT for everything else: Chasidut, Kabbalah, Midrash,
                // Responsa, Jewish Thought, etc.
                //
                // Sefaria always divides texts into discrete numbered array
                // entries — each entry is a self-contained unit (a teaching,
                // a story, a responsum paragraph, a midrashic passage). These
                // units must stay intact as individual chunks so that the Hebrew
                // and English versions of the same passage share the same
                // position-based ref and can be cross-referenced by the agent.
                //
                // PROSE concatenation is kept only as the fallback *within*
                // PER_UNIT when a single segment exceeds MAX_TOKENS — see
                // buildChunksFromSegments → buildSplitChunks.
                return Strategy.PER_UNIT;
        }
    }

    // ------------------------------------------------------------------
    // Main entry point
    // ------------------------------------------------------------------

    /**
     * Produce chunks from the walked segments of a text.
     *
     * @param segments   all leaf segments from TextWalker (document order)
     * @param schema     the text's schema (for strategy selection + addressing)
     * @param aliyahRefs aliyah refs from schema.getAliyahRefs() (may be empty)
     * @param template   a partially-filled TextChunk with title/category/language set;
     *                   each produced chunk copies these fields and adds text/ref
     * @return ordered list of chunks for this text
     */
    public List<TextChunk> chunk(List<TextWalker.Segment> segments,
                                  SefariaSchemaJson schema,
                                  List<String> aliyahRefs,
                                  TextChunk template)
    {
        Strategy strategy = selectStrategy(schema);

        List<TextChunk> chunks;
        switch (strategy)
        {
            case ALIYAH:  chunks = chunkByAliyah(segments, aliyahRefs, schema, template); break;
            case CHAPTER: chunks = chunkByChapter(segments, schema, template); break;
            case AMUD:    chunks = chunkByAmud(segments, schema, template); break;
            case PER_UNIT: chunks = chunkPerUnit(segments, schema, template); break;
            default:      chunks = chunkProse(segments, template); break;
        }

        for (int i = 0; i < chunks.size(); i++)
        {
            chunks.get(i).setChunkIndex(i);
            chunks.get(i).computeChunkId();
        }

        log.debug("{} [{}]: {} chunks (strategy={})",
                template.getTitle(), template.getLanguage(), chunks.size(), strategy);
        return chunks;
    }

    // ------------------------------------------------------------------
    // ALIYAH strategy
    // ------------------------------------------------------------------

    private List<TextChunk> chunkByAliyah(List<TextWalker.Segment> segments,
                                           List<String> aliyahRefs,
                                           SefariaSchemaJson schema,
                                           TextChunk template)
    {
        if (aliyahRefs.isEmpty())
        {
            log.warn("No aliyah refs for {}; falling back to chapter chunking", template.getTitle());
            return chunkByChapter(segments, schema, template);
        }

        List<TextChunk> chunks = new ArrayList<>();
        for (String aliyahRef : aliyahRefs)
        {
            int[] range = parseAliyahRange(aliyahRef, template.getTitle());
            if (range == null)
            {
                continue;
            }

            int startChap = range[0], startVerse = range[1];
            int endChap = range[2], endVerse = range[3];

            List<TextWalker.Segment> aliyahSegs = new ArrayList<>();
            for (TextWalker.Segment seg : segments)
            {
                if (seg.indices().size() >= 2)
                {
                    int ch = seg.indices().get(0);
                    int v = seg.indices().get(1);
                    if (isInRange(ch, v, startChap, startVerse, endChap, endVerse))
                    {
                        aliyahSegs.add(seg);
                    }
                }
            }

            if (aliyahSegs.isEmpty())
            {
                continue;
            }

            chunks.addAll(buildChunksFromSegments(
                    aliyahSegs, aliyahRef, schema.getAddressTypes(), template, "ALIYAH"));
        }

        if (chunks.isEmpty())
        {
            log.warn("Aliyah chunking produced nothing for {}; falling back", template.getTitle());
            return chunkByChapter(segments, schema, template);
        }

        return chunks;
    }

    // ------------------------------------------------------------------
    // CHAPTER strategy
    // ------------------------------------------------------------------

    private List<TextChunk> chunkByChapter(List<TextWalker.Segment> segments,
                                            SefariaSchemaJson schema,
                                            TextChunk template)
    {
        return groupByFirstIndex(segments, schema.getAddressTypes(), template, "CHAPTER");
    }

    // ------------------------------------------------------------------
    // AMUD strategy
    // ------------------------------------------------------------------

    private List<TextChunk> chunkByAmud(List<TextWalker.Segment> segments,
                                         SefariaSchemaJson schema,
                                         TextChunk template)
    {
        List<List<TextWalker.Segment>> amudGroups = groupByFirstIndexRaw(segments);
        List<TextChunk> chunks = new ArrayList<>();

        for (int i = 0; i < amudGroups.size(); i++)
        {
            List<TextWalker.Segment> amudSegs = new ArrayList<>(amudGroups.get(i));

            // Add 2-line overlap from the next amud for discourse continuity
            if (i + 1 < amudGroups.size())
            {
                List<TextWalker.Segment> nextAmud = amudGroups.get(i + 1);
                int overlap = Math.min(2, nextAmud.size());
                amudSegs.addAll(nextAmud.subList(0, overlap));
            }

            if (amudSegs.isEmpty())
            {
                continue;
            }

            int amudIndex = amudSegs.get(0).indices().get(0);
            String amudStr = RefBuilder.talmudDaf(amudIndex);
            String amudRef = template.getTitle() + " " + amudStr;

            List<TextChunk> amudChunks = buildChunksFromSegments(
                    amudSegs, amudRef, schema.getAddressTypes(), template, "AMUD");

            for (TextChunk c : amudChunks)
            {
                c.setSectionRef(amudRef);
            }
            chunks.addAll(amudChunks);
        }
        return chunks;
    }

    // ------------------------------------------------------------------
    // PER_UNIT strategy
    // ------------------------------------------------------------------

    private List<TextChunk> chunkPerUnit(List<TextWalker.Segment> segments,
                                          SefariaSchemaJson schema,
                                          TextChunk template)
    {
        List<TextChunk> chunks = new ArrayList<>();
        for (TextWalker.Segment seg : segments)
        {
            chunks.addAll(buildChunksFromSegments(
                    Collections.singletonList(seg),
                    seg.ref(),
                    schema.getAddressTypes(),
                    template,
                    "PER_UNIT"));
        }
        return chunks;
    }

    // ------------------------------------------------------------------
    // PROSE strategy
    // ------------------------------------------------------------------

    private List<TextChunk> chunkProse(List<TextWalker.Segment> segments, TextChunk template)
    {
        List<String> sentences = new ArrayList<>();
        for (TextWalker.Segment seg : segments)
        {
            sentences.addAll(splitSentences(seg.text()));
        }

        List<TextChunk> chunks = new ArrayList<>();
        List<String> batch = new ArrayList<>();
        int batchTokens = 0;
        int partNum = 0;

        for (String sentence : sentences)
        {
            int st = estimateTokens(sentence);
            if (batchTokens + st > TARGET_PROSE_TOKENS && !batch.isEmpty())
            {
                TextChunk c = buildChunk(template, String.join(" ", batch), "PROSE");
                c.setRef(partNum == 0 ? template.getTitle()
                        : template.getTitle() + " (part " + (partNum + 1) + ")");
                c.setSectionRef(template.getTitle());
                chunks.add(c);
                partNum++;
                batch = overlapTail(batch, PROSE_OVERLAP_TOKENS);
                batchTokens = estimateTokens(String.join(" ", batch));
            }
            batch.add(sentence);
            batchTokens += st;
        }

        if (!batch.isEmpty())
        {
            TextChunk c = buildChunk(template, String.join(" ", batch), "PROSE");
            c.setRef(partNum == 0 ? template.getTitle()
                    : template.getTitle() + " (part " + (partNum + 1) + ")");
            c.setSectionRef(template.getTitle());
            chunks.add(c);
        }

        return chunks;
    }

    // ------------------------------------------------------------------
    // Shared helpers
    // ------------------------------------------------------------------

    private List<TextChunk> groupByFirstIndex(List<TextWalker.Segment> segments,
                                               List<String> addressTypes,
                                               TextChunk template,
                                               String strategyName)
    {
        List<List<TextWalker.Segment>> groups = groupByFirstIndexRaw(segments);
        List<TextChunk> chunks = new ArrayList<>();

        for (List<TextWalker.Segment> group : groups)
        {
            if (group.isEmpty())
            {
                continue;
            }
            int idx = group.get(0).indices().get(0);
            String addrType = addressTypes.isEmpty() ? "Integer" : addressTypes.get(0);
            String groupRef = RefBuilder.buildSectionRef(template.getTitle(), idx, addrType);
            chunks.addAll(buildChunksFromSegments(group, groupRef, addressTypes,
                    template, strategyName));
        }
        return chunks;
    }

    private List<List<TextWalker.Segment>> groupByFirstIndexRaw(List<TextWalker.Segment> segments)
    {
        List<List<TextWalker.Segment>> groups = new ArrayList<>();
        List<TextWalker.Segment> current = new ArrayList<>();
        int currentIdx = -1;

        for (TextWalker.Segment seg : segments)
        {
            int idx = seg.indices().isEmpty() ? 0 : seg.indices().get(0);
            if (idx != currentIdx)
            {
                if (!current.isEmpty())
                {
                    groups.add(new ArrayList<>(current));
                }
                current.clear();
                currentIdx = idx;
            }
            current.add(seg);
        }
        if (!current.isEmpty())
        {
            groups.add(current);
        }
        return groups;
    }

    private List<TextChunk> buildChunksFromSegments(List<TextWalker.Segment> segs,
                                                     String primaryRef,
                                                     List<String> addressTypes,
                                                     TextChunk template,
                                                     String strategyName)
    {
        String combined = joinSegments(segs);

        if (estimateTokens(combined) <= MAX_TOKENS)
        {
            TextChunk c = buildChunk(template, combined, strategyName);
            c.setRef(primaryRef);
            if (!segs.isEmpty() && !segs.get(0).indices().isEmpty())
            {
                c.setStartIndices(segs.get(0).indices());
                if (segs.get(0).indices().size() > 1)
                {
                    String addrType = addressTypes.isEmpty() ? "Integer" : addressTypes.get(0);
                    c.setSectionRef(RefBuilder.buildSectionRef(
                            template.getTitle(), segs.get(0).indices().get(0), addrType));
                }
                else
                {
                    c.setSectionRef(primaryRef);
                }
            }
            return Collections.singletonList(c);
        }

        // Exceeds cap — split at sentence boundaries
        List<String> sentences = new ArrayList<>();
        for (TextWalker.Segment seg : segs)
        {
            sentences.addAll(splitSentences(seg.text()));
        }
        return buildSplitChunks(sentences, primaryRef, template, strategyName);
    }

    private List<TextChunk> buildSplitChunks(List<String> sentences,
                                              String baseRef,
                                              TextChunk template,
                                              String strategyName)
    {
        List<TextChunk> chunks = new ArrayList<>();
        List<String> batch = new ArrayList<>();
        int batchTokens = 0;
        int partNum = 0;

        for (String sentence : sentences)
        {
            int st = estimateTokens(sentence);
            if (batchTokens + st > MAX_TOKENS && !batch.isEmpty())
            {
                TextChunk c = buildChunk(template, String.join(" ", batch), strategyName);
                c.setRef(partNum == 0 ? baseRef : baseRef + " (part " + (partNum + 1) + ")");
                c.setSectionRef(baseRef);
                chunks.add(c);
                partNum++;
                batch = overlapTail(batch, PROSE_OVERLAP_TOKENS);
                batchTokens = estimateTokens(String.join(" ", batch));
            }
            batch.add(sentence);
            batchTokens += st;
        }

        if (!batch.isEmpty())
        {
            TextChunk c = buildChunk(template, String.join(" ", batch), strategyName);
            c.setRef(partNum == 0 ? baseRef : baseRef + " (part " + (partNum + 1) + ")");
            c.setSectionRef(baseRef);
            chunks.add(c);
        }

        return chunks;
    }

    private TextChunk buildChunk(TextChunk template, String text, String strategy)
    {
        TextChunk c = new TextChunk();
        c.setTitle(template.getTitle());
        c.setLanguage(template.getLanguage());
        c.setVersionTitle("merged");
        c.setCategory(template.getCategory());
        c.setSubcategory(template.getSubcategory());
        c.setFullCategories(template.getFullCategories());
        c.setPrimary(template.isPrimary());
        c.setBaseRef(template.getBaseRef());
        c.setHalakhaTopic(template.getHalakhaTopic());
        c.setChunkStrategy(strategy);
        c.setText(text);
        return c;
    }

    // ------------------------------------------------------------------
    // Aliyah range parsing
    // ------------------------------------------------------------------

    /**
     * Parse "Genesis 1:1-1:5" → [startChap, startVerse, endChap, endVerse].
     * Handles cross-chapter ranges like "Genesis 1:22-2:3".
     */
    private int[] parseAliyahRange(String aliyahRef, String title)
    {
        try
        {
            String loc = aliyahRef;
            if (loc.startsWith(title + " "))
            {
                loc = loc.substring(title.length() + 1);
            }

            // loc = "1:1-1:5" or "1:22-2:3"
            String[] halves = loc.split("-");
            int[] start = parseChVerse(halves[0]);
            int[] end = (halves.length > 1) ? parseChVerse(halves[1]) : start;
            if (start == null || end == null)
            {
                return null;
            }
            return new int[]{start[0], start[1], end[0], end[1]};
        }
        catch (Exception e)
        {
            log.warn("Could not parse aliyah ref: {}", aliyahRef);
            return null;
        }
    }

    private int[] parseChVerse(String s)
    {
        String[] parts = s.trim().split(":");
        if (parts.length < 2)
        {
            return null;
        }
        return new int[]{Integer.parseInt(parts[0].trim()), Integer.parseInt(parts[1].trim())};
    }

    private boolean isInRange(int ch, int v, int sc, int sv, int ec, int ev)
    {
        if (ch < sc || ch > ec)
        {
            return false;
        }
        if (ch == sc && v < sv)
        {
            return false;
        }
        if (ch == ec && v > ev)
        {
            return false;
        }
        return true;
    }

    // ------------------------------------------------------------------
    // Utility
    // ------------------------------------------------------------------

    static int estimateTokens(String text)
    {
        if (text == null || text.isBlank())
        {
            return 0;
        }
        return (int) (text.split("\\s+").length * 1.3);
    }

    private List<String> splitSentences(String text)
    {
        // Split at whitespace that immediately follows sentence-ending punctuation.
        //
        // The lookbehind (?<=[.!?;׃]) is fixed-length (1 char), so the regex
        // runs in O(n) time regardless of content. The previous form used a
        // variable-length lookbehind (\\s+ inside (?<=...)) which caused
        // catastrophic backtracking on long texts with few sentence boundaries.
        //
        // Covers: English (. ! ?), clause breaks (;), Hebrew sof-pasuk (׃).
        String[] raw = SENTENCE_SPLIT.split(text);
        List<String> result = new ArrayList<>();
        for (String s : raw)
        {
            if (!s.isBlank())
            {
                result.add(s.trim());
            }
        }
        if (result.isEmpty() && !text.isBlank())
        {
            result.add(text.trim());
        }
        return result;
    }

    private List<String> overlapTail(List<String> sentences, int targetTokens)
    {
        List<String> tail = new ArrayList<>();
        int tokens = 0;
        for (int i = sentences.size() - 1; i >= 0; i--)
        {
            int t = estimateTokens(sentences.get(i));
            if (tokens + t > targetTokens)
            {
                break;
            }
            tail.add(0, sentences.get(i));
            tokens += t;
        }
        return tail;
    }

    private String joinSegments(List<TextWalker.Segment> segs)
    {
        StringBuilder sb = new StringBuilder();
        for (TextWalker.Segment seg : segs)
        {
            if (sb.length() > 0)
            {
                sb.append(" ");
            }
            sb.append(seg.text());
        }
        return sb.toString();
    }
}
