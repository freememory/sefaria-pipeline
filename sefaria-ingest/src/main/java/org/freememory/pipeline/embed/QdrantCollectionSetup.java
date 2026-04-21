package org.freememory.pipeline.embed;

import io.qdrant.client.QdrantClient;
import io.qdrant.client.grpc.Collections.Distance;
import io.qdrant.client.grpc.Collections.PayloadSchemaType;
import io.qdrant.client.grpc.Collections.VectorParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates and configures the Qdrant collection for Sefaria text chunks.
 *
 * === Collection design ===
 *
 * A single collection "sefaria_texts" holds all chunks regardless of language
 * or category. Filtering is done at query time via payload indexes.
 *
 * Vector space: text-embedding-3-small produces 1536-dimensional vectors.
 * Distance metric: cosine — standard for semantic text similarity.
 *
 * === Payload indexes ===
 *
 * Indexes are created on the fields most commonly used for filtering at
 * query time. Without indexes these filters are O(n) full-collection scans.
 *
 *   category    Keyword   — filter to a corpus section (Talmud, Tanakh, ...)
 *   language    Keyword   — "en" or "he" filters
 *   title       Keyword   — exact-title lookups for citation retrieval
 *   ref         Keyword   — canonical ref lookup (e.g. "Shabbat 2a")
 *   is_primary  Bool      — primary texts vs. commentaries
 */
public class QdrantCollectionSetup
{
    private static final Logger log = LoggerFactory.getLogger(QdrantCollectionSetup.class);

    /** Dimensionality of text-embedding-3-small. */
    public static final int VECTOR_SIZE = 1536;

    /**
     * Ensures the named collection exists with the correct configuration.
     * If it already exists this is a no-op — safe to call on every startup.
     */
    public static void ensureCollection(QdrantClient client, String collectionName)
            throws Exception
    {
        boolean exists = client.collectionExistsAsync(collectionName).get();

        if (exists)
        {
            log.info("Collection '{}' already exists — skipping creation.", collectionName);
            return;
        }

        log.info("Creating collection '{}' (size={}, distance=Cosine)...",
                collectionName, VECTOR_SIZE);

        client.createCollectionAsync(
            collectionName,
            VectorParams.newBuilder()
                .setSize(VECTOR_SIZE)
                .setDistance(Distance.Cosine)
                .build()
        ).get();

        log.info("Collection created. Building payload indexes...");
        createIndexes(client, collectionName);
        log.info("Collection '{}' ready.", collectionName);
    }

    // ------------------------------------------------------------------
    // Private
    // ------------------------------------------------------------------

    private static void createIndexes(QdrantClient client, String collectionName)
            throws Exception
    {
        keywordIndex(client, collectionName, "category");
        keywordIndex(client, collectionName, "language");
        keywordIndex(client, collectionName, "title");
        keywordIndex(client, collectionName, "ref");
        boolIndex(client, collectionName, "is_primary");
    }

    private static void keywordIndex(QdrantClient client,
                                     String collectionName,
                                     String field) throws Exception
    {
        client.createPayloadIndexAsync(
            collectionName,
            field,
            PayloadSchemaType.Keyword,
            null, true, null, null
        ).get();
        log.debug("  Keyword index: {}", field);
    }

    private static void boolIndex(QdrantClient client,
                                  String collectionName,
                                  String field) throws Exception
    {
        client.createPayloadIndexAsync(
            collectionName,
            field,
            PayloadSchemaType.Bool,
            null, true, null, null
        ).get();
        log.debug("  Bool index: {}", field);
    }
}
