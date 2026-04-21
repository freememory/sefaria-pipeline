package org.freememory.pipeline.download;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Vert.x Verticle that downloads per-text schema JSON files from the GCS bucket.
 *
 * === Schema Files ===
 *
 * Schema files live at:
 *   https://storage.googleapis.com/sefaria-export/schemas/{Title}.json
 *
 * They contain critical structural metadata used by the processing phase:
 *   - depth, addressTypes, sectionNames  — how to walk the text array
 *   - alt_structs.Parasha               — aliyah divisions for Torah books
 *   - categories                         — the text's place in the corpus hierarchy
 *   - enDesc                             — English description
 *
 * === Deduplication ===
 *
 * One schema file covers all languages of a text (the structure is the same
 * whether you're reading the Hebrew or English version of Genesis). So we
 * deduplicate by title — downloading each schema only once regardless of how
 * many BookEntry rows reference that title.
 *
 * === URL Encoding ===
 *
 * Titles like "Mishneh Torah, Prayer" contain commas and spaces that must be
 * URL-encoded. We use URLEncoder and replace '+' with '%20' to get the GCS-
 * compatible path encoding.
 */
public class SchemaFetcher extends AbstractVerticle
{
    private static final Logger log = LoggerFactory.getLogger(SchemaFetcher.class);
    private static final String SCHEMA_BASE_URL =
            "https://storage.googleapis.com/sefaria-export/schemas/";

    private final Path schemasDir;
    private final List<BookEntry> entries;
    private final int concurrency;
    private final Runnable onComplete;

    private WebClient webClient;

    public SchemaFetcher(Path schemasDir,
                         List<BookEntry> entries,
                         int concurrency,
                         Runnable onComplete)
    {
        this.schemasDir = schemasDir;
        this.entries = entries;
        this.concurrency = concurrency;
        this.onComplete = onComplete;
    }

    @Override
    public void start(Future<Void> startFuture)
    {
        WebClientOptions options = new WebClientOptions()
                .setMaxPoolSize(concurrency)
                .setConnectTimeout(30000);
        webClient = WebClient.create(vertx, options);

        // Deduplicate by title
        Set<String> titles = entries.stream()
                .map(BookEntry::getTitle)
                .collect(Collectors.toSet());

        log.info("Fetching schemas for {} unique titles", titles.size());

        AtomicInteger downloaded = new AtomicInteger(0);
        AtomicInteger skipped = new AtomicInteger(0);
        List<String> titleList = List.copyOf(titles);

        fetchBatch(titleList, 0, downloaded, skipped, startFuture);
    }

    private void fetchBatch(List<String> titles, int offset,
                             AtomicInteger downloaded, AtomicInteger skipped,
                             Future<Void> startFuture)
    {
        if (offset >= titles.size())
        {
            log.info("Schema fetch complete: {} downloaded, {} skipped",
                    downloaded.get(), skipped.get());
            onComplete.run();
            startFuture.complete();
            return;
        }

        int end = Math.min(offset + concurrency, titles.size());
        List<String> batch = titles.subList(offset, end);

        List<Future> batchFutures = batch.stream()
                .map(title -> fetchSchema(title, downloaded, skipped))
                .collect(Collectors.toList());

        CompositeFuture.join(batchFutures).setHandler(ar ->
                fetchBatch(titles, offset + batch.size(), downloaded, skipped, startFuture));
    }

    private Future<Void> fetchSchema(String title,
                                      AtomicInteger downloaded,
                                      AtomicInteger skipped)
    {
        Future<Void> future = Future.future();
        Path dest = schemasDir.resolve(sanitizeFilename(title) + ".json");

        if (Files.exists(dest))
        {
            skipped.incrementAndGet();
            future.complete();
            return future;
        }

        vertx.executeBlocking(
            blocking ->
            {
                try
                {
                    Files.createDirectories(dest.getParent());
                    blocking.complete();
                }
                catch (Exception e)
                {
                    blocking.fail(e);
                }
            },
            dirResult ->
            {
                String url = SCHEMA_BASE_URL + encodeTitle(title) + ".json";
                webClient.getAbs(url)
                        .timeout(30000)
                        .send(ar ->
                        {
                            if (ar.failed() || ar.result().statusCode() == 404)
                            {
                                // Schema missing — not an error
                                future.complete();
                                return;
                            }
                            if (ar.result().statusCode() != 200)
                            {
                                log.warn("Schema HTTP {} for {}", ar.result().statusCode(), title);
                                future.complete();
                                return;
                            }

                            Buffer body = ar.result().body();
                            vertx.executeBlocking(
                                blocking ->
                                {
                                    try
                                    {
                                        Path tmp = dest.resolveSibling(dest.getFileName() + ".tmp");
                                        Files.write(tmp, body.getBytes());
                                        Files.move(tmp, dest,
                                                java.nio.file.StandardCopyOption.REPLACE_EXISTING,
                                                java.nio.file.StandardCopyOption.ATOMIC_MOVE);
                                        blocking.complete();
                                    }
                                    catch (Exception e)
                                    {
                                        blocking.fail(e);
                                    }
                                },
                                writeResult ->
                                {
                                    if (writeResult.succeeded())
                                    {
                                        downloaded.incrementAndGet();
                                    }
                                    future.complete();
                                }
                            );
                        });
            }
        );

        return future;
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    /** Returns the local path to a schema file, or null if not yet downloaded. */
    public Path getSchemaPath(String title)
    {
        Path p = schemasDir.resolve(sanitizeFilename(title) + ".json");
        return Files.exists(p) ? p : null;
    }

    private static String sanitizeFilename(String title)
    {
        // Use underscores for spaces so local filenames match the GCS bucket naming
        // convention (schemas/Song_of_Songs.json) and also match what ProcessScript
        // expects when it looks up schemas by title.
        return title.replace(" ", "_").replace("/", "_").replace(":", "_")
                    .replace("\"", "").replace("?", "");
    }

    private static String encodeTitle(String title)
    {
        // GCS schema filenames use underscores for spaces, not %20.
        // Other special characters (commas, parentheses, apostrophes) are kept
        // literal in the bucket object names and are valid in URL paths.
        return title.replace(" ", "_");
    }
}
