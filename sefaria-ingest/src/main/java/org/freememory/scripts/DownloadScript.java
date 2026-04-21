package org.freememory.scripts;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import org.freememory.config.ConfigLoader;
import org.freememory.config.PipelineConfig;
import org.freememory.config.PipelineConfig.DownloadConfig;
import org.freememory.pipeline.download.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Phase 1 entrypoint: Download the Sefaria corpus to local disk.
 *
 * === What this does ===
 *
 * 1. Loads books.json (the 19,643-entry master catalog)
 * 2. Filters to merged English + Hebrew entries for the configured priority tiers
 * 3. Downloads text JSON files from the GCS public bucket (Vert.x WebClient)
 * 4. Downloads per-text schema JSON files
 * 5. Downloads the links CSV files (~650MB total)
 * 6. Optionally builds the SQLite links index (links.db)
 *
 * === Configuration ===
 *
 * All options are set in a JSON config file. Pass it with --config:
 *
 *   java -jar sefaria-pipeline.jar --config config/download-p0.json
 *
 * If --config is omitted, the script looks for config/pipeline.json in the
 * current directory, then falls back to built-in defaults.
 *
 * See src/main/resources/config/ for example config files.
 *
 * === Resume support ===
 *
 * Safe to re-run: files already present on disk are skipped automatically.
 */
public class DownloadScript
{
    private static final Logger log = LoggerFactory.getLogger(DownloadScript.class);

    public static void main(String[] args) throws Exception
    {
        PipelineConfig config = ConfigLoader.load(args);
        DownloadConfig dl = config.getDownload();

        log.info("=== Sefaria Download Script ===");
        log.info("Priorities:  {}", dl.getPriorities().isEmpty() ? "ALL" : dl.getPriorities());
        log.info("Output:      {}", dl.outputDirPath());
        log.info("Concurrency: {}", dl.getConcurrency());
        log.info("Build index: {}", dl.isBuildIndex());

        // Step 1: Load catalog
        BooksJsonCatalog catalog = loadCatalog(dl);
        log.info("Catalog loaded: {} total entries", catalog.size());

        // Step 2: Select entries to download
        List<BookEntry> toDownload = selectEntries(catalog, dl);
        log.info("Selected {} entries for download", toDownload.size());

        // Step 3+: Deploy Vert.x verticles in sequence
        //
        // Vert.x 3.x uses Netty's async DNS resolver by default. On Windows (and
        // some Linux setups with VPNs) Netty exhausts its per-hostname query limit
        // before it can reach external hosts, producing:
        //   "failed to resolve '...' Exceeded max queries per resolve N"
        // Setting this property makes Vert.x fall back to the JDK's standard
        // blocking DNS resolver, which honours the OS resolver correctly.
        System.setProperty("vertx.disableDnsResolver", "true");

        Vertx vertx = Vertx.vertx(new VertxOptions().setWorkerPoolSize(10));

        vertx.deployVerticle(
            new SefariaCrawler(dl.outputDirPath(), toDownload, dl.getConcurrency(), downloadCount ->
            {
                if (!dl.isSkipSchemas())
                {
                    vertx.deployVerticle(
                        new SchemaFetcher(dl.schemasDirPath(), toDownload, dl.getConcurrency(), () ->
                        {
                            afterSchemas(dl, toDownload, vertx);
                        }),
                        r -> { if (r.failed()) { log.error("SchemaFetcher failed", r.cause()); vertx.close(); } }
                    );
                }
                else
                {
                    afterSchemas(dl, toDownload, vertx);
                }
            }),
            r -> { if (r.failed()) { log.error("SefariaCrawler failed", r.cause()); vertx.close(); } }
        );

        Thread.sleep(Long.MAX_VALUE);
    }

    private static void afterSchemas(DownloadConfig dl, List<BookEntry> toDownload, Vertx vertx)
    {
        if (!dl.isSkipLinks())
        {
            vertx.deployVerticle(
                new LinksDownloader(dl.linksDirPath(), () -> buildIndexAndShutdown(dl, vertx)),
                r -> { if (r.failed()) { log.error("LinksDownloader failed", r.cause()); vertx.close(); } }
            );
        }
        else
        {
            buildIndexAndShutdown(dl, vertx);
        }
    }

    private static void buildIndexAndShutdown(DownloadConfig dl, Vertx vertx)
    {
        if (dl.isBuildIndex())
        {
            log.info("Building SQLite links index...");
            vertx.executeBlocking(
                blocking ->
                {
                    try
                    {
                        LinksIndexBuilder builder = new LinksIndexBuilder(
                                dl.linksDirPath(), dl.linksDbPath());
                        builder.build(dl.isForceIndex());
                        blocking.complete();
                    }
                    catch (Exception e)
                    {
                        blocking.fail(e);
                    }
                },
                result ->
                {
                    if (result.failed())
                    {
                        log.error("Links index build failed: {}", result.cause().getMessage());
                    }
                    log.info("=== Phase 1 complete. Run ProcessScript next. ===");
                    vertx.close();
                    System.exit(result.failed() ? 1 : 0);
                }
            );
        }
        else
        {
            log.info("=== Phase 1 complete. Run ProcessScript next. ===");
            vertx.close();
            System.exit(0);
        }
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    private static BooksJsonCatalog loadCatalog(DownloadConfig dl) throws Exception
    {
        if (dl.booksJsonPath() != null)
        {
            return BooksJsonCatalog.fromLocalFile(dl.booksJsonPath());
        }

        // Auto-detect the sibling Sefaria-Export repository
        java.nio.file.Path[] candidates = {
            java.nio.file.Path.of("../Sefaria-Export/books.json"),
            java.nio.file.Path.of("../../Sefaria-Export/books.json"),
            java.nio.file.Path.of("books.json")
        };

        for (java.nio.file.Path candidate : candidates)
        {
            if (candidate.toFile().exists())
            {
                log.info("Using books.json at {}", candidate.toAbsolutePath());
                return BooksJsonCatalog.fromLocalFile(candidate);
            }
        }

        throw new RuntimeException(
            "books.json not found. Set \"download.booksJson\" in your config, "
          + "or clone Sefaria-Export alongside this project.");
    }

    private static List<BookEntry> selectEntries(BooksJsonCatalog catalog, DownloadConfig dl)
    {
        if (dl.getPriorities().isEmpty())
        {
            return catalog.getAllPrioritized();
        }
        return dl.getPriorities().stream()
                .flatMap(p -> catalog.getByPriority(p).stream())
                .distinct()
                .collect(Collectors.toList());
    }
}
