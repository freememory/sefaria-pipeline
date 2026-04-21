package org.freememory.scripts;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import org.freememory.config.ConfigLoader;
import org.freememory.config.PipelineConfig;
import org.freememory.config.PipelineConfig.ProcessConfig;
import org.freememory.pipeline.process.ChunkEnricher;
import org.freememory.pipeline.process.TextChunk;
import org.freememory.pipeline.process.TextFileProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Phase 2 entrypoint: Process downloaded text files into JSONL chunks.
 *
 * === What this does ===
 *
 * For each merged.json file found under the input directory:
 *   1. Parses the Sefaria text JSON
 *   2. Loads the corresponding schema (for depth, addressTypes, aliyot)
 *   3. Walks the jagged text array to extract leaf segments
 *   4. Chunks segments using the appropriate strategy for the text type
 *   5. Enriches chunks with linked refs from links.db
 *   6. Appends chunks as JSONL to data/processed/{Category}_{lang}.jsonl
 *
 * === Configuration ===
 *
 *   java -cp sefaria-pipeline.jar org.sefaria.scripts.ProcessScript \
 *        --config config/pipeline.json
 *
 * See src/main/resources/config/ for example config files.
 *
 * === Output ===
 *
 * One JSONL file per (top-level category, language) pair:
 *   data/processed/Tanakh_en.jsonl
 *   data/processed/Talmud_he.jsonl
 *   ...
 *
 * Each line is a JSON-serialised TextChunk with all metadata fields populated.
 */
public class ProcessScript extends AbstractVerticle
{
    private static final Logger log = LoggerFactory.getLogger(ProcessScript.class);

    private final ProcessConfig config;
    private final ObjectMapper mapper = new ObjectMapper();

    public ProcessScript(ProcessConfig config)
    {
        this.config = config;
    }

    @Override
    public void start(Future<Void> startFuture)
    {
        vertx.executeBlocking(
            blocking ->
            {
                try
                {
                    runProcessing();
                    blocking.complete();
                }
                catch (Exception e)
                {
                    blocking.fail(e);
                }
            },
            result ->
            {
                if (result.succeeded())
                {
                    log.info("=== Phase 2 complete. Run EmbedIngestScript next. ===");
                    startFuture.complete();
                }
                else
                {
                    log.error("Processing failed", result.cause());
                    startFuture.fail(result.cause());
                }
            }
        );
    }

    // ------------------------------------------------------------------
    // Main processing loop (runs in worker thread via executeBlocking)
    // ------------------------------------------------------------------

    private void runProcessing() throws Exception
    {
        Files.createDirectories(config.outputDirPath());

        List<Path> textFiles = findTextFiles(config.inputDirPath());
        log.info("Found {} text files to process", textFiles.size());

        if (textFiles.isEmpty())
        {
            log.warn("No text files found in {}. Run DownloadScript first.", config.inputDirPath());
            return;
        }

        ChunkEnricher enricher = openEnricher();
        TextFileProcessor processor = new TextFileProcessor();
        AtomicInteger processed = new AtomicInteger(0);
        AtomicInteger totalChunks = new AtomicInteger(0);
        int total = textFiles.size();

        for (Path textFile : textFiles)
        {
            try
            {
                List<TextChunk> chunks = processFile(textFile, processor);
                if (!chunks.isEmpty())
                {
                    if (enricher != null)
                    {
                        enricher.enrichAll(chunks);
                    }
                    writeChunks(chunks);
                    totalChunks.addAndGet(chunks.size());
                }

                int n = processed.incrementAndGet();
                if (n % 100 == 0 || n == total)
                {
                    log.info("Progress: {}/{} files, {} chunks total", n, total, totalChunks.get());
                }
            }
            catch (Exception e)
            {
                log.warn("Error processing {}: {}", textFile, e.getMessage());
            }
        }

        if (enricher != null)
        {
            enricher.close();
        }

        log.info("Processing complete: {} files → {} chunks in {}",
                processed.get(), totalChunks.get(), config.outputDirPath());
    }

    // ------------------------------------------------------------------
    // Per-file processing
    // ------------------------------------------------------------------

    private List<TextChunk> processFile(Path textFile, TextFileProcessor processor) throws Exception
    {
        List<String> pathParts = getRelativePathParts(textFile);
        List<String> categories = deriveCategories(pathParts);

        // Title is the second-to-last path component (before the language dir)
        String title = pathParts.size() >= 2 ? pathParts.get(pathParts.size() - 2) : "";
        Path schemaFile = config.schemasDirPath().resolve(sanitize(title) + ".json");
        if (!Files.exists(schemaFile))
        {
            schemaFile = null;
        }

        return processor.process(textFile, schemaFile, categories);
    }

    // ------------------------------------------------------------------
    // JSONL output
    // ------------------------------------------------------------------

    private void writeChunks(List<TextChunk> chunks) throws Exception
    {
        Map<String, List<TextChunk>> byFile = new LinkedHashMap<>();
        for (TextChunk chunk : chunks)
        {
            String key = sanitize(chunk.getCategory()) + "_" + chunk.getLanguage();
            byFile.computeIfAbsent(key, k -> new ArrayList<>()).add(chunk);
        }

        for (Map.Entry<String, List<TextChunk>> entry : byFile.entrySet())
        {
            Path outFile = config.outputDirPath().resolve(entry.getKey() + ".jsonl");
            try (BufferedWriter writer = Files.newBufferedWriter(
                    outFile, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND))
            {
                for (TextChunk chunk : entry.getValue())
                {
                    writer.write(mapper.writeValueAsString(chunk));
                    writer.newLine();
                }
            }
        }
    }

    // ------------------------------------------------------------------
    // Path utilities
    // ------------------------------------------------------------------

    private List<Path> findTextFiles(Path root) throws Exception
    {
        try (Stream<Path> stream = Files.walk(root))
        {
            return stream
                    .filter(p -> p.getFileName().toString().equals("merged.json"))
                    .sorted()
                    .collect(Collectors.toList());
        }
    }

    private List<String> getRelativePathParts(Path textFile)
    {
        Path relative = config.inputDirPath().relativize(textFile.getParent());
        List<String> parts = new ArrayList<>();
        for (Path p : relative)
        {
            parts.add(p.toString());
        }
        return parts;
    }

    private List<String> deriveCategories(List<String> pathParts)
    {
        // Path layout: [Category1, Category2, ..., Title, Language]
        // Categories are everything except the last two components.
        if (pathParts.size() <= 2)
        {
            return pathParts.isEmpty() ? List.of() : List.of(pathParts.get(0));
        }
        return pathParts.subList(0, pathParts.size() - 2);
    }

    private static String sanitize(String s)
    {
        if (s == null)
        {
            return "unknown";
        }
        return s.replace("/", "_").replace(":", "_").replace(" ", "_");
    }

    private ChunkEnricher openEnricher()
    {
        Path dbPath = config.linksDbPath();
        if (!Files.exists(dbPath))
        {
            log.warn("links.db not found at {}. Enrichment skipped.", dbPath);
            return null;
        }
        try
        {
            ChunkEnricher enricher = new ChunkEnricher(dbPath);
            log.info("Links enricher ready ({})", dbPath);
            return enricher;
        }
        catch (Exception e)
        {
            log.warn("Could not open links.db: {}. Enrichment skipped.", e.getMessage());
            return null;
        }
    }

    // ------------------------------------------------------------------
    // Static main
    // ------------------------------------------------------------------

    public static void main(String[] args) throws Exception
    {
        PipelineConfig config = ConfigLoader.load(args);
        ProcessConfig pc = config.getProcess();

        log.info("=== Sefaria Process Script ===");
        log.info("Input:    {}", pc.inputDirPath());
        log.info("Schemas:  {}", pc.schemasDirPath());
        log.info("Links DB: {}", pc.linksDbPath());
        log.info("Output:   {}", pc.outputDirPath());

        Vertx vertx = Vertx.vertx(new VertxOptions().setWorkerPoolSize(4));
        vertx.deployVerticle(
            new ProcessScript(pc),
            result ->
            {
                if (result.failed())
                {
                    log.error("ProcessScript failed", result.cause());
                }
                vertx.close();
                System.exit(result.failed() ? 1 : 0);
            }
        );

        Thread.sleep(Long.MAX_VALUE);
    }
}
