package org.freememory.pipeline.download;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Vert.x Verticle that downloads Sefaria text files from the GCS public bucket.
 *
 * === How it works ===
 *
 * Downloads are performed via the Vert.x WebClient (non-blocking HTTP).
 * Entries are processed in batches of size `concurrency` — once a batch
 * of CompositeFutures completes, the next batch begins. This naturally
 * rate-limits the number of in-flight requests without threads or semaphores.
 *
 * === Resume support ===
 *
 * Before downloading a file, the verticle checks whether it already exists
 * on disk and appears to be a valid JSON file (starts with '{' or '[').
 * If so, it is skipped. This means the download can be safely re-run after
 * an interruption without re-downloading completed files.
 *
 * === Retry ===
 *
 * Each failed download is retried up to MAX_RETRIES times with exponential
 * backoff using vertx.setTimer(). Network errors and non-200 responses both
 * trigger retries. 404 responses are silently skipped (file not in bucket).
 *
 * === Usage ===
 *
 *   Vertx vertx = Vertx.vertx();
 *   vertx.deployVerticle(new SefariaCrawler(outputRoot, entries, concurrency, done -> {
 *       vertx.close();
 *   }));
 */
public class SefariaCrawler extends AbstractVerticle
{
    private static final Logger log = LoggerFactory.getLogger(SefariaCrawler.class);
    private static final int MAX_RETRIES = 3;

    private final Path outputRoot;
    private final List<BookEntry> entries;
    private final int concurrency;
    private final java.util.function.Consumer<Integer> onComplete;

    private WebClient webClient;
    private final AtomicInteger downloaded = new AtomicInteger(0);
    private final AtomicInteger skipped = new AtomicInteger(0);
    private final AtomicInteger failed = new AtomicInteger(0);

    public SefariaCrawler(Path outputRoot,
                          List<BookEntry> entries,
                          int concurrency,
                          java.util.function.Consumer<Integer> onComplete)
    {
        this.outputRoot = outputRoot;
        this.entries = entries;
        this.concurrency = concurrency;
        this.onComplete = onComplete;
    }

    @Override
    public void start(Future<Void> startFuture)
    {
        WebClientOptions options = new WebClientOptions()
                .setMaxPoolSize(concurrency)
                .setConnectTimeout(30000)
                .setIdleTimeout(60);

        webClient = WebClient.create(vertx, options);

        log.info("Starting download of {} entries (concurrency={})", entries.size(), concurrency);

        processBatches(entries, 0, startFuture);
    }

    // ------------------------------------------------------------------
    // Batch processing
    // ------------------------------------------------------------------

    private void processBatches(List<BookEntry> allEntries, int offset, Future<Void> startFuture)
    {
        if (offset >= allEntries.size())
        {
            log.info("Download complete: {} downloaded, {} skipped, {} failed",
                    downloaded.get(), skipped.get(), failed.get());
            onComplete.accept(downloaded.get());
            startFuture.complete();
            return;
        }

        int end = Math.min(offset + concurrency, allEntries.size());
        List<BookEntry> batch = allEntries.subList(offset, end);

        List<Future> batchFutures = batch.stream()
                .map(entry -> downloadEntry(entry, 1))
                .collect(Collectors.toList());

        CompositeFuture.join(batchFutures).setHandler(ar ->
        {
            int nextOffset = offset + batch.size();
            if (nextOffset % 200 < concurrency)
            {
                log.info("Progress: {}/{} — downloaded={}, skipped={}, failed={}",
                        nextOffset, allEntries.size(),
                        downloaded.get(), skipped.get(), failed.get());
            }
            // Always continue regardless of individual failures (join = wait for all)
            processBatches(allEntries, nextOffset, startFuture);
        });
    }

    // ------------------------------------------------------------------
    // Single file download with retry
    // ------------------------------------------------------------------

    private Future<Void> downloadEntry(BookEntry entry, int attempt)
    {
        Future<Void> future = Future.future();
        Path dest = outputRoot.resolve(entry.getRelativePath());

        // Resume: skip if already valid
        if (isValidJson(dest))
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
                if (dirResult.failed())
                {
                    future.fail(dirResult.cause());
                    return;
                }
                doHttpGet(entry.getJsonUrl(), dest, entry, attempt, future);
            }
        );

        return future;
    }

    /**
     * Percent-encode a URL that may contain raw spaces or Unicode in its path.
     *
     * books.json stores URLs with unencoded titles, e.g.:
     *   .../Abarbanel on I Kings/English/merged.json
     * GCS rejects these with HTTP 400. We re-encode using the URL + URI trick:
     * URL parses leniently, then the multi-arg URI constructor encodes each
     * component without double-encoding anything already percent-encoded.
     */
    private static String encodeUrl(String raw)
    {
        try
        {
            URL u = new URL(raw);
            URI uri = new URI(u.getProtocol(), u.getAuthority(), u.getPath(), u.getQuery(), null);
            return uri.toASCIIString();
        }
        catch (Exception e)
        {
            return raw; // fall back to original if parsing fails
        }
    }

    private void doHttpGet(String url, Path dest, BookEntry entry, int attempt, Future<Void> future)
    {
        webClient.getAbs(encodeUrl(url))
                .timeout(60000)
                .send(ar ->
                {
                    if (ar.failed())
                    {
                        retryOrFail(url, dest, entry, attempt, ar.cause(), future);
                        return;
                    }

                    HttpResponse<Buffer> response = ar.result();

                    if (response.statusCode() == 404)
                    {
                        // File absent from bucket — not an error
                        log.debug("404 (not in bucket): {}", entry.getTitle());
                        skipped.incrementAndGet();
                        future.complete();
                        return;
                    }

                    if (response.statusCode() != 200)
                    {
                        retryOrFail(url, dest, entry, attempt,
                                new RuntimeException("HTTP " + response.statusCode()), future);
                        return;
                    }

                    // Write to disk (blocking — use executeBlocking)
                    Buffer body = response.body();
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
                                future.complete();
                            }
                            else
                            {
                                future.fail(writeResult.cause());
                            }
                        }
                    );
                });
    }

    private void retryOrFail(String url, Path dest, BookEntry entry,
                              int attempt, Throwable cause, Future<Void> future)
    {
        if (attempt >= MAX_RETRIES)
        {
            log.warn("Failed after {} attempts: {} — {}", attempt, entry.getTitle(), cause.getMessage());
            failed.incrementAndGet();
            future.complete(); // don't propagate; let other downloads continue
            return;
        }

        long backoffMs = (long) Math.pow(2, attempt) * 1000;
        log.warn("Attempt {}/{} failed for {}: {}. Retrying in {}ms",
                attempt, MAX_RETRIES, entry.getTitle(), cause.getMessage(), backoffMs);

        vertx.setTimer(backoffMs, timerId -> doHttpGet(url, dest, entry, attempt + 1, future));
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    private boolean isValidJson(Path path)
    {
        try
        {
            if (!Files.exists(path) || Files.size(path) < 10)
            {
                return false;
            }
            byte[] head = new byte[1];
            try (java.io.InputStream in = Files.newInputStream(path))
            {
                if (in.read(head) < 1)
                {
                    return false;
                }
            }
            return head[0] == '{' || head[0] == '[';
        }
        catch (Exception e)
        {
            return false;
        }
    }
}
