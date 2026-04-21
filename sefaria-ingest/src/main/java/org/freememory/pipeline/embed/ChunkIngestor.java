package org.freememory.pipeline.embed;

import dev.langchain4j.data.embedding.Embedding;
import dev.langchain4j.data.segment.TextSegment;
import dev.langchain4j.model.embedding.EmbeddingModel;
import dev.langchain4j.model.output.Response;
import io.qdrant.client.PointIdFactory;
import io.qdrant.client.QdrantClient;
import io.qdrant.client.ValueFactory;
import io.qdrant.client.VectorsFactory;
import io.qdrant.client.grpc.JsonWithInt.Value;
import io.qdrant.client.grpc.Common.PointId;
import io.qdrant.client.grpc.Points.PointStruct;
import io.qdrant.client.grpc.Points.RetrievedPoint;
import org.freememory.pipeline.process.TextChunk;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Embeds and upserts TextChunks into Qdrant.
 *
 * === Processing pipeline for one batch ===
 *
 *   1. Derive a deterministic UUID for each chunk from its chunk_id
 *      (SHA-256 hex string → type-3 UUID via nameUUIDFromBytes).
 *   2. Optionally query Qdrant to find which UUIDs already exist,
 *      and skip those chunks (avoids re-billing the OpenAI API on re-runs).
 *   3. Call OpenAI text-embedding-3-small on the remaining texts.
 *   4. Build PointStruct objects with all chunk metadata as payload fields.
 *   5. Upsert to Qdrant (idempotent — safe to call more than once for the
 *      same point ID; later upsert simply overwrites earlier data).
 *
 * === Payload fields stored per point ===
 *
 *   chunk_id, ref, section_ref, title, language, category, subcategory,
 *   is_primary, text, base_ref, linked_refs (list), token_estimate
 *
 * These match the indexed fields created by QdrantCollectionSetup, enabling
 * fast payload-filtered searches at query time.
 *
 * === Thread safety ===
 *
 * Not thread-safe. Designed for single-threaded sequential use from
 * EmbedIngestScript. The AtomicLong counters are safe to read from any thread.
 */
public class ChunkIngestor
{
    private static final Logger log = LoggerFactory.getLogger(ChunkIngestor.class);

    /**
     * Maximum characters per chunk before truncation.
     *
     * OpenAI's per-item limit for text-embedding-3-small is 8,191 tokens.
     *
     * For English text, String.length() ≈ token count / 4, so 8,000 chars is
     * far under the limit. For Hebrew text with nikkud (vowel-pointing marks),
     * the relationship breaks down: each nikkud is a separate Unicode code point
     * (1 Java char), but in UTF-8 it is 2 bytes, and cl100k_base may not have
     * merged those byte pairs into a single token. In the worst case, every
     * character — base letter and nikkud alike — becomes 2 tokens, giving a
     * 2:1 token-to-char ratio. At that ratio, 8,000 chars → 16,000 tokens,
     * far over the 8,191 limit.
     *
     * Setting the cap to 4,000 chars guarantees safety at a 2:1 ratio:
     *   4,000 × 2 = 8,000 tokens < 8,191 limit
     *
     * Chunks exceeding this limit are rare (long prose paragraphs) and are
     * truncated with a warning. Semantic quality is minimally impacted.
     */
    private static final int MAX_CHARS_PER_CHUNK = 4_000;

    private final QdrantClient qdrant;
    private final EmbeddingModel embeddingModel;
    private final String collectionName;
    private final boolean skipExisting;

    private final AtomicLong ingested = new AtomicLong(0);
    private final AtomicLong skipped  = new AtomicLong(0);
    private final AtomicLong errors   = new AtomicLong(0);

    public ChunkIngestor(QdrantClient qdrant,
                         EmbeddingModel embeddingModel,
                         String collectionName,
                         boolean skipExisting)
    {
        this.qdrant         = qdrant;
        this.embeddingModel = embeddingModel;
        this.collectionName = collectionName;
        this.skipExisting   = skipExisting;
    }

    // ------------------------------------------------------------------
    // Public API
    // ------------------------------------------------------------------

    /**
     * Embed and upsert a batch of chunks.
     *
     * Errors are logged and counted rather than propagated so a single bad
     * batch cannot abort the entire pipeline run. However, the two failure
     * modes are treated differently:
     *
     *   - Failure DURING embedding (OpenAI call): no charge was incurred.
     *     The chunks will simply be retried on the next run at no extra cost.
     *
     *   - Failure AFTER embedding but DURING upsert (Qdrant): OpenAI has
     *     already billed for those vectors. The upsert is retried up to
     *     {@value #UPSERT_MAX_ATTEMPTS} times before giving up. If all retries
     *     fail the chunk IDs are logged at ERROR level so you can identify them.
     *     On the next run they will be re-embedded (unavoidable double charge),
     *     but this should be extremely rare with retry in place.
     */
    public void ingestBatch(List<TextChunk> batch)
    {
        if (batch.isEmpty())
        {
            return;
        }

        try
        {
            doIngestBatch(batch);
        }
        catch (Exception e)
        {
            errors.addAndGet(batch.size());
            log.error("Batch ingest failed ({} chunks starting at '{}'): {}",
                    batch.size(),
                    batch.get(0).getRef(),
                    e.getMessage());
        }
    }

    // ------------------------------------------------------------------
    // Internal
    // ------------------------------------------------------------------

    private void doIngestBatch(List<TextChunk> batch) throws Exception
    {
        // Step 1 — Build chunk_id → UUID map (deterministic, stable across runs)
        Map<String, UUID> uuidByChunkId = new LinkedHashMap<>();
        for (TextChunk chunk : batch)
        {
            UUID uuid = UUID.nameUUIDFromBytes(
                    chunk.getChunkId().getBytes(StandardCharsets.UTF_8));
            uuidByChunkId.put(chunk.getChunkId(), uuid);
        }

        // Step 2 — Optionally filter to chunks not yet in Qdrant
        List<TextChunk> toEmbed = skipExisting
                ? filterNew(batch, uuidByChunkId)
                : new ArrayList<>(batch);

        int skipCount = batch.size() - toEmbed.size();
        if (skipCount > 0)
        {
            skipped.addAndGet(skipCount);
            log.debug("  Skipped {} already-ingested chunks in this batch", skipCount);
        }

        // Step 3 — Filter and cap chunks
        // - Drop blank chunks (nothing to embed)
        // - Truncate chunks that exceed OpenAI's per-item token limit.
        //   The per-item limit is 8,191 tokens. For nikkud-heavy Hebrew text each
        //   Unicode code point ≈ 1 BPE token, so String.length() is a reliable
        //   proxy. We truncate at MAX_CHARS_PER_CHUNK characters to stay safe.
        List<TextChunk> embeddable = new ArrayList<>();
        for (TextChunk chunk : toEmbed)
        {
            String text = chunk.getText();
            if (text == null || text.isBlank())
            {
                log.debug("  Skipping blank chunk: {}", chunk.getRef());
                skipped.incrementAndGet();
                continue;
            }
            if (text.length() > MAX_CHARS_PER_CHUNK)
            {
                log.warn("  Truncating oversized chunk ({} chars → {} chars, ~{}K tokens max) ref={}",
                        text.length(), MAX_CHARS_PER_CHUNK, MAX_CHARS_PER_CHUNK * 2 / 1000,
                        chunk.getRef());
                chunk.setText(text.substring(0, MAX_CHARS_PER_CHUNK));
            }
            embeddable.add(chunk);
        }

        if (embeddable.isEmpty())
        {
            return;
        }

        // Step 4 — Embed via OpenAI (with adaptive rate-limit retry and
        //           per-item oversized-chunk recovery).
        //
        // If OpenAI rejects a specific input[N] as too long, we log the
        // offending chunk's ID, ref, and char count, drop it from the batch,
        // and retry the remaining chunks. One bad chunk cannot kill the batch.
        List<TextSegment> segments = new ArrayList<>();
        for (TextChunk chunk : embeddable)
        {
            segments.add(TextSegment.from(chunk.getText()));
        }

        Response<List<Embedding>> response = null;
        while (response == null)
        {
            try
            {
                response = embedWithRateLimitRetry(segments);
            }
            catch (Exception e)
            {
                int badIndex = parseInputIndex(e.getMessage());
                if (badIndex < 0 || badIndex >= embeddable.size())
                {
                    throw e; // not an oversized-item error we can recover from
                }

                TextChunk bad = embeddable.get(badIndex);
                int charLen = bad.getText() != null ? bad.getText().length() : 0;
                log.error("OVERSIZED chunk dropped: chunk_id={} ref=\"{}\" chars={}",
                        bad.getChunkId(), bad.getRef(), charLen);
                errors.incrementAndGet();

                embeddable.remove(badIndex);
                segments.remove(badIndex);

                if (embeddable.isEmpty())
                {
                    return; // nothing left to embed
                }
            }
        }

        List<Embedding> embeddings = response.content();

        if (embeddings.size() != embeddable.size())
        {
            throw new IllegalStateException(
                    "Embedding count mismatch: expected " + embeddable.size()
                    + " but got " + embeddings.size());
        }

        // Step 5 — Build and upsert PointStructs
        List<PointStruct> points = new ArrayList<>();
        for (int i = 0; i < embeddable.size(); i++)
        {
            TextChunk chunk  = embeddable.get(i);
            UUID uuid        = uuidByChunkId.get(chunk.getChunkId());
            float[] vector   = embeddings.get(i).vector();

            PointStruct point = PointStruct.newBuilder()
                    .setId(PointIdFactory.id(uuid))
                    .setVectors(VectorsFactory.vectors(vector))
                    .putAllPayload(buildPayload(chunk))
                    .build();
            points.add(point);
        }

        // OpenAI has already billed for the embeddings above — the upsert must
        // not silently drop them. Retry with backoff before giving up so that a
        // transient Qdrant hiccup doesn't cause a double-charge on the next run.
        upsertWithRetry(points);
        ingested.addAndGet(embeddable.size());
        log.debug("  Upserted {} points", embeddable.size());
    }

    // ------------------------------------------------------------------
    // Upsert with retry
    // ------------------------------------------------------------------

    /** Number of times to attempt a Qdrant upsert before giving up. */
    private static final int UPSERT_MAX_ATTEMPTS = 4;

    /** Base delay between upsert retries in milliseconds (doubles each attempt). */
    private static final long UPSERT_RETRY_BASE_MS = 2_000;

    /**
     * Attempt the Qdrant upsert up to {@value #UPSERT_MAX_ATTEMPTS} times.
     *
     * Retry delays: 2 s → 4 s → 8 s (exponential backoff).
     * Throws on final failure so the caller can log which chunk IDs were lost.
     */
    private void upsertWithRetry(List<PointStruct> points) throws Exception
    {
        Exception lastError = null;

        for (int attempt = 1; attempt <= UPSERT_MAX_ATTEMPTS; attempt++)
        {
            try
            {
                qdrant.upsertAsync(collectionName, points).get();
                return; // success
            }
            catch (Exception e)
            {
                lastError = e;
                if (attempt < UPSERT_MAX_ATTEMPTS)
                {
                    long delayMs = UPSERT_RETRY_BASE_MS * (1L << (attempt - 1)); // 2s, 4s, 8s
                    log.warn("Qdrant upsert failed (attempt {}/{}), retrying in {} s: {}",
                            attempt, UPSERT_MAX_ATTEMPTS, delayMs / 1000, e.getMessage());
                    Thread.sleep(delayMs);
                }
            }
        }

        throw new RuntimeException(
                "Qdrant upsert failed after " + UPSERT_MAX_ATTEMPTS + " attempts — "
                + points.size() + " embeddings lost. These chunks will be re-embedded on "
                + "the next run.", lastError);
    }

    // ------------------------------------------------------------------
    // Adaptive rate-limit retry for embedding
    // ------------------------------------------------------------------

    /**
     * Maximum times to retry an embedding call after a rate-limit response.
     * Each retry sleeps for exactly the duration OpenAI specifies in the error
     * message, plus a small buffer — so recovery is as fast as possible.
     */
    private static final int EMBED_MAX_RETRIES = 10;

    /** Extra buffer added on top of OpenAI's suggested retry-after time. */
    private static final long EMBED_RETRY_BUFFER_MS = 500;

    /** Fallback sleep when we can't parse the retry-after duration. */
    private static final long EMBED_FALLBACK_SLEEP_MS = 5_000;

    /** Matches "Please try again in 2.348s" in OpenAI rate-limit error messages. */
    private static final Pattern RETRY_AFTER_PATTERN =
            Pattern.compile("try again in (\\d+\\.?\\d*)s");

    /** Matches "Invalid 'input[17]': maximum input length" — per-item size errors. */
    private static final Pattern INPUT_INDEX_PATTERN =
            Pattern.compile("input\\[(\\d+)\\].*maximum input length");

    /**
     * Call {@code embedAll} and retry on rate-limit (HTTP 429) responses.
     *
     * Unlike LangChain4j's built-in retry (fixed short delays, limited attempts),
     * this loop parses OpenAI's suggested wait time from the error message and
     * sleeps for exactly that duration before retrying. This is optimal: we
     * waste no time sleeping longer than necessary, and we never give up too early.
     *
     * Non-rate-limit exceptions propagate immediately so the caller can distinguish
     * genuine failures (bad API key, server error) from transient throttling.
     */
    private Response<List<Embedding>> embedWithRateLimitRetry(List<TextSegment> segments)
            throws Exception
    {
        int attempt = 0;
        while (true)
        {
            try
            {
                return embeddingModel.embedAll(segments);
            }
            catch (Exception e)
            {
                long waitMs = parseRetryAfterMs(e);
                if (waitMs < 0)
                {
                    throw e; // not a rate-limit error — propagate immediately
                }

                attempt++;
                if (attempt >= EMBED_MAX_RETRIES)
                {
                    throw new RuntimeException(
                            "Rate limit retry exhausted after " + EMBED_MAX_RETRIES
                            + " attempts", e);
                }

                log.warn("Rate limited (attempt {}/{}), sleeping {}ms — {}",
                        attempt, EMBED_MAX_RETRIES, waitMs,
                        summariseRateLimit(e.getMessage()));
                Thread.sleep(waitMs);
            }
        }
    }

    /**
     * Determine the retry delay for a failed embedding call.
     *
     * Checks both the exception type hierarchy (reliable) and the message text
     * (for OpenAI API-level errors that arrive as HTTP response bodies).
     *
     * @return milliseconds to sleep before retrying, or -1 if this error is not
     *         retryable (e.g. invalid API key, bad request)
     */
    private static long parseRetryAfterMs(Exception exception)
    {
        // --- 1. Walk the cause chain and check exception types ---
        //
        // This is the reliable path for low-level network failures.
        // e.getMessage() alone is insufficient: openai4j may wrap the cause
        // as new RuntimeException(cause), in which case getMessage() returns
        // cause.toString() — but may also use new RuntimeException(cause.getMessage())
        // which gives only the message without the class name, causing a
        // message-string check to miss the "UnknownHostException" substring.
        for (Throwable cause = exception; cause != null; cause = cause.getCause())
        {
            if (cause instanceof java.net.UnknownHostException
                    || cause instanceof java.net.SocketException
                    || cause instanceof java.net.ConnectException
                    || cause instanceof java.net.NoRouteToHostException)
            {
                return 15_000; // DNS / network outage — give the network time to recover
            }
            if (cause instanceof java.io.InterruptedIOException)
            {
                return 5_000; // read/write timeout
            }
        }

        // --- 2. Message-based detection for OpenAI API-level errors ---
        //
        // These arrive as structured HTTP 4xx/5xx responses and are reported
        // via RuntimeException with the JSON body as the message string.
        String message = exception.getMessage();
        if (message == null)
        {
            return -1;
        }

        // Rate-limit (429): parse OpenAI's suggested wait time for exact recovery.
        if (message.contains("rate_limit_exceeded"))
        {
            Matcher m = RETRY_AFTER_PATTERN.matcher(message);
            if (m.find())
            {
                double seconds = Double.parseDouble(m.group(1));
                return (long) (seconds * 1_000) + EMBED_RETRY_BUFFER_MS;
            }
            return EMBED_FALLBACK_SLEEP_MS;
        }

        // Server-side overload / transient errors reported in the response body.
        if (message.contains("502") || message.contains("503") || message.contains("529")
                || message.contains("overloaded") || message.contains("timeout")
                || message.contains("InterruptedIOException")
                || message.contains("UnknownHostException") // belt-and-suspenders
                || message.contains("SocketException")
                || message.contains("Connection reset"))
        {
            return 5_000;
        }

        return -1; // not retryable
    }

    /**
     * Parse the batch index from an OpenAI per-item length error.
     *
     * @return the 0-based index of the offending input, or -1 if not a length error
     */
    private static int parseInputIndex(String message)
    {
        if (message == null)
        {
            return -1;
        }
        Matcher m = INPUT_INDEX_PATTERN.matcher(message);
        return m.find() ? Integer.parseInt(m.group(1)) : -1;
    }

    /** Extract a short human-readable summary from a rate-limit error for logging. */
    private static String summariseRateLimit(String message)
    {
        if (message == null)
        {
            return "(no message)";
        }
        // Show only the inner "message" field from the JSON error body, not the full JSON.
        int start = message.indexOf("\"message\":");
        if (start >= 0)
        {
            int end = message.indexOf("\",", start);
            if (end < 0)
            {
                end = message.indexOf("\"", start + 12);
            }
            if (end > start + 12)
            {
                return message.substring(start + 12, end).trim();
            }
        }
        return message.length() > 120 ? message.substring(0, 120) + "…" : message;
    }

    /**
     * Query Qdrant for the batch's UUIDs and return only the chunks whose
     * UUID was not found (i.e. not yet ingested).
     */
    private List<TextChunk> filterNew(List<TextChunk> batch,
                                       Map<String, UUID> uuidByChunkId) throws Exception
    {
        List<PointId> ids = new ArrayList<>();
        for (UUID uuid : uuidByChunkId.values())
        {
            ids.add(PointIdFactory.id(uuid));
        }

        // Retrieve only IDs (no payload, no vectors) — just existence check.
        // boolean overload: withPayload=false, withVectors=false, readConsistency=null.
        List<RetrievedPoint> found = qdrant.retrieveAsync(
                collectionName, ids, false, false, null).get();

        Set<String> existingUuids = new HashSet<>();
        for (RetrievedPoint rp : found)
        {
            existingUuids.add(rp.getId().getUuid());
        }

        List<TextChunk> newChunks = new ArrayList<>();
        for (TextChunk chunk : batch)
        {
            UUID uuid = uuidByChunkId.get(chunk.getChunkId());
            if (!existingUuids.contains(uuid.toString()))
            {
                newChunks.add(chunk);
            }
        }
        return newChunks;
    }

    // ------------------------------------------------------------------
    // Payload builder
    // ------------------------------------------------------------------

    /**
     * Convert a TextChunk to the Qdrant payload map.
     *
     * Only non-null / non-blank fields are included. All payload fields that
     * have a corresponding index in QdrantCollectionSetup must be stored here
     * with consistent field names (category, language, title, ref, is_primary).
     */
    private Map<String, Value> buildPayload(TextChunk chunk)
    {
        Map<String, Value> payload = new LinkedHashMap<>();

        putStr(payload, "chunk_id",   chunk.getChunkId());
        putStr(payload, "ref",        chunk.getRef());
        putStr(payload, "title",      chunk.getTitle());
        putStr(payload, "language",   chunk.getLanguage());
        putStr(payload, "category",   chunk.getCategory());
        putStr(payload, "subcategory", chunk.getSubcategory());
        payload.put("is_primary",     ValueFactory.value(chunk.isPrimary()));

        if (chunk.getSectionRef() != null && !chunk.getSectionRef().isBlank())
        {
            putStr(payload, "section_ref", chunk.getSectionRef());
        }
        if (chunk.getText() != null)
        {
            putStr(payload, "text", chunk.getText());
        }
        if (chunk.getBaseRef() != null && !chunk.getBaseRef().isBlank())
        {
            putStr(payload, "base_ref", chunk.getBaseRef());
        }
        if (chunk.getLinkedRefs() != null && !chunk.getLinkedRefs().isEmpty())
        {
            List<Value> refs = new ArrayList<>();
            for (String r : chunk.getLinkedRefs())
            {
                refs.add(ValueFactory.value(r));
            }
            payload.put("linked_refs", ValueFactory.list(refs));
        }

        payload.put("token_estimate", ValueFactory.value((long) chunk.getTokenEstimate()));

        return payload;
    }

    private void putStr(Map<String, Value> map, String key, String val)
    {
        if (val != null)
        {
            map.put(key, ValueFactory.value(val));
        }
    }

    // ------------------------------------------------------------------
    // Counters
    // ------------------------------------------------------------------

    public long getIngested() { return ingested.get(); }
    public long getSkipped()  { return skipped.get(); }
    public long getErrors()   { return errors.get(); }
}
