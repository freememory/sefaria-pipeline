package org.freememory.scripts;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.langchain4j.model.embedding.EmbeddingModel;
import dev.langchain4j.model.openai.OpenAiEmbeddingModel;
import io.qdrant.client.QdrantClient;
import io.qdrant.client.QdrantGrpcClient;
import org.freememory.config.ConfigLoader;
import org.freememory.config.PipelineConfig;
import org.freememory.config.PipelineConfig.EmbedConfig;
import org.freememory.pipeline.embed.ChunkIngestor;
import org.freememory.pipeline.embed.QdrantCollectionSetup;
import org.freememory.pipeline.process.TextChunk;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Phase 3 entrypoint: embed chunks and ingest into Qdrant.
 *
 * === What this does ===
 *
 *   For each JSONL file produced by Phase 2 (data/processed/*.jsonl):
 *     1. Read chunks line-by-line (streaming — files can be hundreds of MB)
 *     2. Accumulate into batches of {batchSize} (default 100)
 *     3. For each batch: optionally skip already-ingested chunks,
 *        embed the rest with OpenAI text-embedding-3-small, upsert to Qdrant
 *
 * === Prerequisites ===
 *
 *   - Qdrant running locally:
 *       docker run -d -p 6333:6333 -p 6334:6334 \
 *           -v /path/to/qdrant_storage:/qdrant/storage qdrant/qdrant
 *
 *   - OpenAI API key set:
 *       set OPENAI_API_KEY=sk-...      (Windows CMD)
 *       export OPENAI_API_KEY=sk-...   (bash/PowerShell)
 *
 * === Usage ===
 *
 *   java -cp sefaria-pipeline.jar org.sefaria.scripts.EmbedIngestScript
 *   java -cp sefaria-pipeline.jar org.sefaria.scripts.EmbedIngestScript --config config/pipeline.json
 *
 * === Resume / re-run safety ===
 *
 *   With skipExisting=true (default), already-ingested chunks are detected
 *   by querying Qdrant for their point UUID before embedding. Re-running the
 *   script after an interruption will pick up where it left off without
 *   re-billing the OpenAI API for chunks already in the database.
 *
 * === Cost estimate ===
 *
 *   Full corpus (~1M chunks × 300 tokens avg = 300M tokens):
 *     300M × $0.02/1M = ~$6 for all embeddings.
 *   P0 only (~250K chunks): ~$1.50.
 *
 * === Notes on plain-Java design ===
 *
 *   Unlike Phases 1–2, this script does not use Vert.x. The embedding and
 *   Qdrant upsert calls are inherently sequential (each batch must complete
 *   before the next is submitted), so an event loop provides no benefit.
 *   Plain blocking Java keeps the code simple.
 */
public class EmbedIngestScript
{
    private static final Logger log = LoggerFactory.getLogger(EmbedIngestScript.class);

    public static void main(String[] args) throws Exception
    {
        PipelineConfig config = ConfigLoader.load(args);
        EmbedConfig ec = config.getEmbed();

        log.info("=== Sefaria Embed + Ingest Script ===");
        log.info("Input dir:     {}", ec.inputDirPath());
        log.info("Model:         {}", ec.getEmbeddingModel());
        log.info("Batch size:    {}", ec.getBatchSize());
        log.info("Qdrant:        {}:{}", ec.getQdrantHost(), ec.getQdrantPort());
        log.info("Collection:    {}", ec.getCollectionName());
        log.info("Skip existing: {}", ec.isSkipExisting());

        // Resolve OpenAI API key: config file takes priority, env var is the fallback.
        // This lets developers store the key in their local config JSON without
        // needing to set an environment variable each session.
        String openAiKey = ec.getOpenAiApiKey();
        if (openAiKey == null || openAiKey.isBlank())
        {
            openAiKey = System.getenv("OPENAI_API_KEY");
        }
        if (openAiKey == null || openAiKey.isBlank())
        {
            log.error("No OpenAI API key found.");
            log.error("Either set 'embed.openAiApiKey' in your config file,");
            log.error("or set the OPENAI_API_KEY environment variable.");
            System.exit(1);
        }

        // Connect to Qdrant (gRPC on port 6334 by default)
        QdrantClient qdrant = new QdrantClient(
                QdrantGrpcClient.newBuilder(ec.getQdrantHost(), ec.getQdrantPort(), false)
                        .build()
        );

        try
        {
            // Ensure the collection and its payload indexes exist
            QdrantCollectionSetup.ensureCollection(qdrant, ec.getCollectionName());

            // Create the embedding model (LangChain4j wraps the OpenAI API).
            // maxRetries(0) disables LangChain4j's built-in retry loop — we handle
            // rate-limit retries and oversized-chunk recovery ourselves in ChunkIngestor,
            // which gives cleaner logs and smarter back-off behaviour.
            EmbeddingModel embeddingModel = OpenAiEmbeddingModel.builder()
                    .apiKey(openAiKey)
                    .modelName(ec.getEmbeddingModel())
                    .maxRetries(0)
                    .build();

            ChunkIngestor ingestor = new ChunkIngestor(
                    qdrant,
                    embeddingModel,
                    ec.getCollectionName(),
                    ec.isSkipExisting()
            );

            // Discover all JSONL files
            List<Path> jsonlFiles = findJsonlFiles(ec.inputDirPath());
            if (jsonlFiles.isEmpty())
            {
                log.warn("No JSONL files found in {}. Run ProcessScript first.",
                        ec.inputDirPath());
                return;
            }

            log.info("Found {} JSONL files to process", jsonlFiles.size());
            log.info("Batch limits: {} chunks OR {} chars (whichever comes first)",
                    ec.getBatchSize(), ec.getMaxBatchChars());
            log.info("Inter-batch delay: {} ms  (set embed.batchDelayMs=0 to disable)",
                    ec.getBatchDelayMs());
            ObjectMapper mapper = new ObjectMapper();
            long totalChunksRead = 0;
            int fileNum = 0;

            for (Path jsonlFile : jsonlFiles)
            {
                fileNum++;
                log.info("[{}/{}] Processing {}  ({} MB)",
                        fileNum,
                        jsonlFiles.size(),
                        jsonlFile.getFileName(),
                        Files.size(jsonlFile) / (1024 * 1024));

                long fileChunks = processFile(
                        jsonlFile, mapper, ingestor,
                        ec.getBatchSize(), ec.getMaxBatchChars(), ec.getBatchDelayMs());
                totalChunksRead += fileChunks;

                log.info("  Done: {} chunks in file | running totals — ingested={} skipped={} errors={}",
                        fileChunks,
                        ingestor.getIngested(),
                        ingestor.getSkipped(),
                        ingestor.getErrors());
            }

            log.info("=== Phase 3 complete ===");
            log.info("Files processed:  {}", jsonlFiles.size());
            log.info("Total chunks read: {}", totalChunksRead);
            log.info("Ingested:         {}", ingestor.getIngested());
            log.info("Skipped:          {}", ingestor.getSkipped());
            log.info("Errors:           {}", ingestor.getErrors());
            log.info("Run RunAgentScript next to start the agent.");
        }
        finally
        {
            qdrant.close();
        }
    }

    // ------------------------------------------------------------------
    // File processing
    // ------------------------------------------------------------------

    /**
     * Stream a single JSONL file line-by-line, batch up chunks, and flush each
     * batch to the ingestor.
     *
     * === Why character count, not word count ===
     *
     * OpenAI's hard limit is 300,000 tokens per embedding request. Our stored
     * {@code tokenEstimate} (wordCount × 1.3) is accurate for English but badly
     * underestimates Hebrew text with nikkud (vowel-pointing diacritics): each
     * diacritic is a separate Unicode code point and a separate BPE token, but
     * {@code split("\\s+")} treats the whole pointed word as one "word".
     *
     * {@code String.length()} counts every code point, including each diacritic,
     * and therefore correlates directly with actual BPE token count for Hebrew.
     * We use it as the budget metric with {@code maxBatchChars = 200,000} chars,
     * which maps to ≤200,000 actual tokens — a comfortable margin under 300,000.
     *
     * @return number of chunk lines successfully parsed from the file
     */
    private static long processFile(Path jsonlFile,
                                     ObjectMapper mapper,
                                     ChunkIngestor ingestor,
                                     int batchSize,
                                     int maxBatchChars,
                                     int batchDelayMs) throws Exception
    {
        long count = 0;
        List<TextChunk> batch = new ArrayList<>(batchSize);
        int batchChars = 0;

        try (BufferedReader reader = Files.newBufferedReader(jsonlFile, StandardCharsets.UTF_8))
        {
            String line;
            while ((line = reader.readLine()) != null)
            {
                if (line.isBlank())
                {
                    continue;
                }

                try
                {
                    TextChunk chunk = mapper.readValue(line, TextChunk.class);
                    int chunkChars = chunk.getText() != null ? chunk.getText().length() : 0;

                    // Flush current batch before adding this chunk if it would push
                    // the character budget over the limit.
                    if (!batch.isEmpty() && batchChars + chunkChars > maxBatchChars)
                    {
                        int flushedChars = batchChars;
                        ingestor.ingestBatch(batch);
                        batch.clear();
                        batchChars = 0;
                        sleepBetweenBatches(flushedChars, maxBatchChars, batchDelayMs);
                    }

                    batch.add(chunk);
                    batchChars += chunkChars;
                    count++;

                    // Secondary cap: max chunks per batch regardless of char count.
                    if (batch.size() >= batchSize)
                    {
                        int flushedChars = batchChars;
                        ingestor.ingestBatch(batch);
                        batch.clear();
                        batchChars = 0;
                        sleepBetweenBatches(flushedChars, maxBatchChars, batchDelayMs);
                    }
                }
                catch (Exception e)
                {
                    log.warn("Failed to parse JSON line (skipping): {}", e.getMessage());
                }
            }
        }

        // Flush the final partial batch
        if (!batch.isEmpty())
        {
            ingestor.ingestBatch(batch);
        }

        return count;
    }

    // ------------------------------------------------------------------
    // Rate-limit helpers
    // ------------------------------------------------------------------

    /**
     * Sleep proportionally to the batch size to avoid saturating the OpenAI TPM limit.
     *
     * Rather than sleeping the same fixed duration after every batch, we scale the
     * sleep to the fraction of the maximum batch size this batch actually used:
     *
     *   sleep = (actualChars / maxBatchChars) × fullBatchDelayMs
     *
     * Examples with maxBatchChars=200,000 and fullBatchDelayMs=12,000:
     *   200K chars (max Hebrew batch) → 12,000 ms
     *    30K chars (typical English)  →  1,800 ms
     *    10K chars (short paragraphs) →    600 ms
     *
     * This keeps total token throughput at ~1M tokens/min regardless of batch
     * composition, while letting small-batch files (English prose) process much
     * faster than large-batch files (nikkud-heavy Hebrew).
     *
     * @param actualChars     characters in the batch that was just submitted
     * @param maxBatchChars   the configured maxBatchChars ceiling
     * @param fullBatchDelayMs delay for a full maxBatchChars batch; 0 disables sleeping
     */
    private static void sleepBetweenBatches(int actualChars, int maxBatchChars, int fullBatchDelayMs)
    {
        if (fullBatchDelayMs <= 0 || actualChars <= 0)
        {
            return;
        }
        long delayMs = (long) ((double) actualChars / maxBatchChars * fullBatchDelayMs);
        if (delayMs < 200)
        {
            return; // not worth sleeping for tiny batches
        }
        try
        {
            Thread.sleep(delayMs);
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
        }
    }

    // ------------------------------------------------------------------
    // Filesystem helpers
    // ------------------------------------------------------------------

    private static List<Path> findJsonlFiles(Path dir) throws Exception
    {
        if (!Files.exists(dir))
        {
            log.warn("Input directory does not exist: {}", dir);
            return List.of();
        }

        try (Stream<Path> stream = Files.list(dir))
        {
            return stream
                    .filter(p -> p.getFileName().toString().endsWith(".jsonl"))
                    .sorted()
                    .collect(Collectors.toList());
        }
    }
}
