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
import io.qdrant.client.grpc.Points.ScoredPoint;
import io.qdrant.client.grpc.Points.SearchPoints;
import io.qdrant.client.grpc.Points.WithPayloadSelector;
import io.qdrant.client.grpc.JsonWithInt.Value;
import org.freememory.config.PipelineConfig.RetrievalConfig;
import org.freememory.pipeline.agent.debug.DebugCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
 * 3. Format each scored point as a citation block:
 *
 *      [Genesis 1:1-8 | Tanakh | en]
 *      In the beginning God created the heavens and the earth…
 *
 * 4. Return as a list of {@link Content} objects for LangChain4j to inject into
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

    private final QdrantClient   qdrant;
    private final EmbeddingModel embeddingModel;
    private final String         collectionName;
    private final RetrievalConfig config;

    public SefariaRetriever(QdrantClient qdrant,
                            EmbeddingModel embeddingModel,
                            String collectionName,
                            RetrievalConfig config)
    {
        this.qdrant          = qdrant;
        this.embeddingModel  = embeddingModel;
        this.collectionName  = collectionName;
        this.config          = config;
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

        // 2. Build search request
        SearchPoints.Builder search = SearchPoints.newBuilder()
                .setCollectionName(collectionName)
                .addAllVector(toFloatList(vector))
                .setLimit(config.getTopK())
                .setWithPayload(WithPayloadSelector.newBuilder().setEnable(true).build());

        Filter filter = buildFilter();
        if (filter != null)
        {
            search.setFilter(filter);
        }

        // 3. Execute
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

        // 4. Format, record for debug panel, and return
        List<Content> contents = new ArrayList<>();
        List<String> debugRefs = new ArrayList<>();

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

            // Citation header + text block
            String formatted = String.format("[%s | %s | %s | %s]\n%s",
                    ref, title, category, language, text);

            contents.add(Content.from(TextSegment.from(formatted)));
            debugRefs.add(String.format("%s (%s/%s, score=%.3f)",
                    ref, category, language, point.getScore()));
        }

        // Record retrieval summary in the debug trace
        if (!debugRefs.isEmpty())
        {
            DebugCollector.record("📖 Qdrant → " + debugRefs.size() + " chunk(s): "
                    + String.join(", ", debugRefs));
        }
        else
        {
            DebugCollector.record("📖 Qdrant → no results");
        }

        return contents;
    }

    // ------------------------------------------------------------------
    // Filter construction
    // ------------------------------------------------------------------

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
