package org.freememory.pipeline.agent;

import dev.langchain4j.data.embedding.Embedding;
import dev.langchain4j.data.segment.TextSegment;
import dev.langchain4j.model.embedding.EmbeddingModel;
import dev.langchain4j.rag.content.Content;
import dev.langchain4j.rag.content.retriever.ContentRetriever;
import dev.langchain4j.rag.query.Query;
import io.qdrant.client.QdrantClient;
import io.qdrant.client.grpc.Common.Condition;
import io.qdrant.client.grpc.Common.FieldCondition;
import io.qdrant.client.grpc.Common.Filter;
import io.qdrant.client.grpc.Common.Match;
import io.qdrant.client.grpc.Points.ScrollPoints;
import io.qdrant.client.grpc.Points.ScrollResponse;
import io.qdrant.client.grpc.Points.ScoredPoint;
import io.qdrant.client.grpc.Points.SearchPoints;
import io.qdrant.client.grpc.Points.WithPayloadSelector;
import io.qdrant.client.grpc.JsonWithInt.Value;
import org.freememory.config.PipelineConfig.RetrievalConfig;
import org.freememory.pipeline.agent.debug.DebugCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * LangChain4j {@link ContentRetriever} backed by Qdrant.
 *
 * Each leaf agent gets its own {@code SefariaRetriever} instance configured
 * with the agent's {@link RetrievalConfig} — category filter, language filter,
 * primary-only flag, and topK.
 *
 * === Retrieval flow ===
 *
 * 1. Embed the query text using OpenAI text-embedding-3-small (same model used
 *    at ingest time — vectors are in the same space).
 * 2. Issue a Qdrant vector search with optional payload filters.
 * 3. Two-hop link expansion (if {@code linksDbPath} is provided and the file
 *    exists): for the top-N primary results, look up linked refs in the SQLite
 *    {@code links.db}, then fetch those chunks from Qdrant by ref payload match.
 *    Linked results are appended after primary results and labelled as such.
 * 4. Format each scored point as a citation block:
 *
 *      [Genesis 1:1-8 | Tanakh | en]
 *      In the beginning God created the heavens and the earth…
 *
 * 5. Return as a list of {@link Content} objects for LangChain4j to inject into
 *    the LLM context.
 *
 * === Payload fields used for filtering ===
 *
 *   category   — top-level Sefaria category ("Tanakh", "Talmud", …)
 *   language   — "en" or "he"
 *   is_primary — boolean; true for primary texts, false for commentaries
 *
 * These fields are indexed in Qdrant by {@code QdrantCollectionSetup}, so
 * filtering is O(1) regardless of collection size.
 */
public class SefariaRetriever implements ContentRetriever
{
    private static final Logger log = LoggerFactory.getLogger(SefariaRetriever.class);

    /** How many primary results to expand into linked texts. */
    private static final int LINK_EXPANSION_TOP_N = 3;

    /** Max linked refs to fetch per primary result (avoids exploding context). */
    private static final int MAX_LINKS_PER_REF    = 5;

    private final QdrantClient    qdrant;
    private final EmbeddingModel  embeddingModel;
    private final String          collectionName;
    private final RetrievalConfig config;
    private final Path            linksDbPath;

    public SefariaRetriever(QdrantClient qdrant,
                            EmbeddingModel embeddingModel,
                            String collectionName,
                            RetrievalConfig config,
                            Path linksDbPath)
    {
        this.qdrant         = qdrant;
        this.embeddingModel = embeddingModel;
        this.collectionName = collectionName;
        this.config         = config;
        this.linksDbPath    = linksDbPath;
    }

    // ------------------------------------------------------------------
    // ContentRetriever
    // ------------------------------------------------------------------

    @Override
    public List<Content> retrieve(Query query)
    {
        // topK=0 means this agent deliberately skips auto-RAG and relies on
        // tool calls (e.g. SEFARIA_LOOKUP) for all retrieval instead.
        if (config.getTopK() <= 0)
        {
            return List.of();
        }

        String queryText = query.text();

        // 1. Embed the query
        Embedding queryEmbedding = embeddingModel.embed(queryText).content();
        float[] vector = queryEmbedding.vector();

        List<Float> vectorList = toFloatList(vector);

        // 2. Build primary search request (unfiltered, or with category/primaryOnly
        //    filters but no language restriction — gets the best semantic matches,
        //    which for English queries will mostly be English chunks).
        SearchPoints.Builder search = SearchPoints.newBuilder()
                .setCollectionName(collectionName)
                .addAllVector(vectorList)
                .setLimit(config.getTopK())
                .setWithPayload(WithPayloadSelector.newBuilder().setEnable(true).build());

        Filter filter = buildFilter();
        if (filter != null)
        {
            search.setFilter(filter);
        }

        // 3. Execute primary search
        List<ScoredPoint> results;
        try
        {
            results = qdrant.searchAsync(search.build()).get();
        }
        catch (Exception e)
        {
            log.error("Qdrant search failed for query \"{}\": {}", queryText, e.getMessage());
            return List.of();
        }

        log.debug("Qdrant returned {} results for query \"{}\"",
                results.size(), queryText.length() > 60
                        ? queryText.substring(0, 60) + "…" : queryText);

        // 3b. Bilingual second pass: when no language filter is configured, the
        //     primary search is English-biased because OpenAI's embedding model
        //     assigns higher cosine similarity to language-matching chunks.  Run a
        //     separate Hebrew-only search with half the topK to ensure Hebrew texts
        //     are always represented, then merge — deduplicating by ref so the same
        //     passage doesn't appear twice.
        if (config.getLanguage() == null || config.getLanguage().isBlank())
        {
            results = mergeWithHebrewSearch(results, vectorList);
        }

        // 4. Two-hop link expansion: collect refs from top-N primary results,
        //    look them up in links.db, then fetch the linked chunks from Qdrant.
        //    We track all primary refs so we can deduplicate later.
        Set<String> primaryRefs = new LinkedHashSet<>();
        for (ScoredPoint p : results)
        {
            String r = str(p.getPayload(), "ref");
            if (r != null) primaryRefs.add(r);
        }

        List<ScoredPoint> linkedPoints = fetchLinkedPoints(primaryRefs);

        // 5. Format primary results, record for debug panel, and return
        List<Content>  contents  = new ArrayList<>();
        List<String>   debugRefs = new ArrayList<>();

        for (ScoredPoint point : results)
        {
            Map<String, Value> payload = point.getPayload();
            String text = str(payload, "text");
            if (text == null || text.isBlank())
            {
                continue;
            }

            String ref      = str(payload, "ref");
            String title    = str(payload, "title");
            String category = str(payload, "category");
            String language = str(payload, "language");

            String formatted = String.format("[%s | %s | %s | %s]\n%s",
                    ref, title, category, language, text);

            contents.add(Content.from(TextSegment.from(formatted)));
            debugRefs.add(String.format("%s (%s/%s, score=%.3f)",
                    ref, category, language, point.getScore()));
        }

        // Append linked results (deduplication against primary set already done
        // inside fetchLinkedPoints).
        List<String> linkedDebugRefs = new ArrayList<>();
        for (ScoredPoint point : linkedPoints)
        {
            Map<String, Value> payload = point.getPayload();
            String text = str(payload, "text");
            if (text == null || text.isBlank())
            {
                continue;
            }

            String ref      = str(payload, "ref");
            String title    = str(payload, "title");
            String category = str(payload, "category");
            String language = str(payload, "language");

            // Mark these explicitly so the LLM knows they were added via link graph
            String formatted = String.format("[%s | %s | %s | %s | linked]\n%s",
                    ref, title, category, language, text);

            contents.add(Content.from(TextSegment.from(formatted)));
            linkedDebugRefs.add(String.format("%s (%s/%s)", ref, category, language));
        }

        // Record retrieval summary in the debug trace
        if (!debugRefs.isEmpty())
        {
            DebugCollector.record("📖 Qdrant primary → " + debugRefs.size()
                    + " chunk(s): " + String.join(", ", debugRefs));
        }
        else
        {
            DebugCollector.record("📖 Qdrant primary → no results");
        }

        if (!linkedDebugRefs.isEmpty())
        {
            DebugCollector.record("🔗 Linked expansion → " + linkedDebugRefs.size()
                    + " chunk(s): " + String.join(", ", linkedDebugRefs));
        }

        return contents;
    }

    // ------------------------------------------------------------------
    // Bilingual retrieval
    // ------------------------------------------------------------------

    /**
     * Runs a secondary Qdrant search restricted to {@code language=he} and
     * merges the results with the primary list.
     *
     * Deduplication is by {@code ref}: if a Hebrew chunk shares a ref with an
     * already-present English chunk it is still included (they are different
     * chunks with different text), but if the exact same chunk appears twice
     * (same ref AND same language) it is deduplicated.
     *
     * The merged list is sorted by score descending so the LLM sees the most
     * relevant chunks first regardless of language.
     */
    private List<ScoredPoint> mergeWithHebrewSearch(List<ScoredPoint> primary,
                                                     List<Float> vectorList)
    {
        int heTopK = Math.max(1, config.getTopK() / 2);

        // Build a Hebrew-only filter, respecting any other active filters
        // (category, primaryOnly) that were applied to the primary search.
        Filter heFilter = buildFilterWithLanguage("he");

        SearchPoints heSearch = SearchPoints.newBuilder()
                .setCollectionName(collectionName)
                .addAllVector(vectorList)
                .setLimit(heTopK)
                .setFilter(heFilter)
                .setWithPayload(WithPayloadSelector.newBuilder().setEnable(true).build())
                .build();

        List<ScoredPoint> heResults;
        try
        {
            heResults = qdrant.searchAsync(heSearch).get();
        }
        catch (Exception e)
        {
            log.warn("Hebrew secondary search failed: {}", e.getMessage());
            return primary;   // fall back to primary-only
        }

        if (heResults.isEmpty())
        {
            return primary;
        }

        // Deduplicate: track ref+language pairs already in the primary list
        Set<String> seen = new LinkedHashSet<>();
        for (ScoredPoint p : primary)
        {
            String ref  = str(p.getPayload(), "ref");
            String lang = str(p.getPayload(), "language");
            if (ref != null) seen.add(ref + "|" + lang);
        }

        List<ScoredPoint> merged = new ArrayList<>(primary);
        for (ScoredPoint p : heResults)
        {
            String ref  = str(p.getPayload(), "ref");
            String lang = str(p.getPayload(), "language");
            String key  = ref + "|" + lang;
            if (!seen.contains(key))
            {
                seen.add(key);
                merged.add(p);
            }
        }

        // Sort by score descending so context is ordered by relevance
        merged.sort((a, b) -> Float.compare(b.getScore(), a.getScore()));
        return merged;
    }

    // ------------------------------------------------------------------
    // Two-hop link expansion
    // ------------------------------------------------------------------

    /**
     * For the top {@value LINK_EXPANSION_TOP_N} refs in {@code primaryRefs},
     * query links.db to find all linked refs, then fetch those chunks from
     * Qdrant by payload match.  Refs already in {@code primaryRefs} are
     * excluded to avoid duplicating context.
     *
     * Returns an empty list if links.db is not configured or does not exist.
     */
    private List<ScoredPoint> fetchLinkedPoints(Set<String> primaryRefs)
    {
        if (linksDbPath == null || !Files.exists(linksDbPath))
        {
            return List.of();
        }

        if (primaryRefs.isEmpty())
        {
            return List.of();
        }

        // Only expand the first LINK_EXPANSION_TOP_N primary refs
        List<String> refsToExpand = primaryRefs.stream()
                .limit(LINK_EXPANSION_TOP_N)
                .toList();

        Set<String> linkedRefs = lookupLinkedRefs(refsToExpand, primaryRefs);
        if (linkedRefs.isEmpty())
        {
            return List.of();
        }

        log.debug("Link expansion: {} primary refs → {} linked refs",
                refsToExpand.size(), linkedRefs.size());

        // Fetch each linked ref from Qdrant via payload match (scroll, not search).
        List<ScoredPoint> points = new ArrayList<>();
        for (String linkedRef : linkedRefs)
        {
            List<ScoredPoint> fetched = fetchByRef(linkedRef);
            points.addAll(fetched);
        }
        return points;
    }

    /**
     * Query links.db for refs linked to any of {@code refsToExpand}.
     * Excludes refs already in {@code excludeRefs} to avoid duplication.
     */
    private Set<String> lookupLinkedRefs(List<String> refsToExpand, Set<String> excludeRefs)
    {
        Set<String> result = new LinkedHashSet<>();

        String url = "jdbc:sqlite:" + linksDbPath.toAbsolutePath();
        String sql = "SELECT ref2, connection_type FROM links WHERE ref1 = ?"
                   + " UNION "
                   + "SELECT ref1, connection_type FROM links WHERE ref2 = ?"
                   + " LIMIT ?";

        try (Connection conn = DriverManager.getConnection(url))
        {
            for (String ref : refsToExpand)
            {
                try (PreparedStatement ps = conn.prepareStatement(sql))
                {
                    ps.setString(1, ref);
                    ps.setString(2, ref);
                    ps.setInt(3, MAX_LINKS_PER_REF);

                    try (ResultSet rs = ps.executeQuery())
                    {
                        while (rs.next())
                        {
                            String linked = rs.getString(1);
                            if (linked != null && !linked.isBlank()
                                    && !excludeRefs.contains(linked))
                            {
                                result.add(linked);
                            }
                        }
                    }
                }
            }
        }
        catch (SQLException e)
        {
            log.warn("links.db query failed: {}", e.getMessage());
        }

        return result;
    }

    /**
     * Fetch all Qdrant points whose {@code ref} payload field exactly matches
     * the given ref string.  Uses a scroll (payload filter) rather than a vector
     * search since we know the exact ref we want.
     *
     * Returns at most a small number of points — typically one per language
     * (one English chunk + one Hebrew chunk for the same ref).
     */
    private List<ScoredPoint> fetchByRef(String ref)
    {
        Filter refFilter = Filter.newBuilder()
                .addMust(keywordMatch("ref", ref))
                .build();

        ScrollPoints scroll = ScrollPoints.newBuilder()
                .setCollectionName(collectionName)
                .setFilter(refFilter)
                .setLimit(4)          // one per language variant is enough
                .setWithPayload(WithPayloadSelector.newBuilder().setEnable(true).build())
                .build();

        try
        {
            ScrollResponse response = qdrant.scrollAsync(scroll).get();
            // ScrollResponse returns RetrievedPoint; wrap as ScoredPoint with score=0
            List<ScoredPoint> result = new ArrayList<>();
            for (var rp : response.getResultList())
            {
                // Convert RetrievedPoint → ScoredPoint (score unused for linked results)
                result.add(ScoredPoint.newBuilder()
                        .setId(rp.getId())
                        .putAllPayload(rp.getPayload())
                        .setScore(0f)
                        .build());
            }
            return result;
        }
        catch (Exception e)
        {
            log.warn("Qdrant scroll for linked ref '{}' failed: {}", ref, e.getMessage());
            return List.of();
        }
    }

    // ------------------------------------------------------------------
    // Filter construction
    // ------------------------------------------------------------------

    /**
     * Variant of {@link #buildFilter()} that forces a specific language,
     * overriding (or adding to) the config language setting.
     * Always returns a non-null filter since at minimum a language clause is added.
     */
    private Filter buildFilterWithLanguage(String language)
    {
        List<Condition> mustClauses = new ArrayList<>();
        mustClauses.add(keywordMatch("language", language));

        if (config.isPrimaryOnly())
        {
            mustClauses.add(boolMatch("is_primary", true));
        }

        List<String> cats = config.getCategories();
        if (cats != null && !cats.isEmpty())
        {
            if (cats.size() == 1)
            {
                mustClauses.add(keywordMatch("category", cats.get(0)));
            }
            else
            {
                List<Condition> shouldClauses = new ArrayList<>();
                for (String cat : cats)
                {
                    shouldClauses.add(keywordMatch("category", cat));
                }
                Filter orFilter = Filter.newBuilder()
                        .addAllShould(shouldClauses)
                        .build();
                mustClauses.add(Condition.newBuilder()
                        .setFilter(orFilter)
                        .build());
            }
        }

        return Filter.newBuilder()
                .addAllMust(mustClauses)
                .build();
    }

    /**
     * Build a Qdrant {@link Filter} from the agent's {@link RetrievalConfig}.
     * Returns {@code null} if no filters are configured (full collection scan).
     */
    private Filter buildFilter()
    {
        List<Condition> mustClauses = new ArrayList<>();

        // Language filter
        String lang = config.getLanguage();
        if (lang != null && !lang.isBlank())
        {
            mustClauses.add(keywordMatch("language", lang));
        }

        // is_primary filter
        if (config.isPrimaryOnly())
        {
            mustClauses.add(boolMatch("is_primary", true));
        }

        // Category filter — if multiple categories, they form a "should" (OR) clause
        List<String> cats = config.getCategories();
        if (cats != null && !cats.isEmpty())
        {
            if (cats.size() == 1)
            {
                mustClauses.add(keywordMatch("category", cats.get(0)));
            }
            else
            {
                // Wrap the OR-group in a nested filter and require it
                List<Condition> shouldClauses = new ArrayList<>();
                for (String cat : cats)
                {
                    shouldClauses.add(keywordMatch("category", cat));
                }
                Filter orFilter = Filter.newBuilder()
                        .addAllShould(shouldClauses)
                        .build();
                mustClauses.add(Condition.newBuilder()
                        .setFilter(orFilter)
                        .build());
            }
        }

        if (mustClauses.isEmpty())
        {
            return null;
        }

        return Filter.newBuilder()
                .addAllMust(mustClauses)
                .build();
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    private static Condition keywordMatch(String field, String value)
    {
        return Condition.newBuilder()
                .setField(FieldCondition.newBuilder()
                        .setKey(field)
                        .setMatch(Match.newBuilder().setKeyword(value).build())
                        .build())
                .build();
    }

    private static Condition boolMatch(String field, boolean value)
    {
        return Condition.newBuilder()
                .setField(FieldCondition.newBuilder()
                        .setKey(field)
                        .setMatch(Match.newBuilder().setBoolean(value).build())
                        .build())
                .build();
    }

    private static List<Float> toFloatList(float[] array)
    {
        List<Float> list = new ArrayList<>(array.length);
        for (float f : array)
        {
            list.add(f);
        }
        return list;
    }

    /** Safely extract a string value from a Qdrant payload map. */
    private static String str(Map<String, Value> payload, String key)
    {
        Value v = payload.get(key);
        return (v != null && v.hasStringValue()) ? v.getStringValue() : null;
    }
}
