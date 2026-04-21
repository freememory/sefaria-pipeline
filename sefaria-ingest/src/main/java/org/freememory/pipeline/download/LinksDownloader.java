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
import java.util.ArrayList;
import java.util.List;

/**
 * Vert.x Verticle that downloads the Sefaria inter-text links CSV files.
 *
 * === What are the links? ===
 *
 * The links data represents cross-references between Jewish texts — the
 * web of citations, commentaries, parallels, and quotations that connects
 * the entire corpus. There are millions of these relationships.
 *
 * The data is split into 16 numbered files (links0.csv through links15.csv)
 * plus aggregate files. Total size: ~650MB compressed on GCS.
 *
 * CSV columns:
 *   Citation 1, Citation 2, Connection Type, Text 1, Text 2, Category 1, Category 2
 *
 * Connection types include: commentary, quotation, reference, parallel, targum, etc.
 *
 * === How they are used ===
 *
 * After downloading, LinksIndexBuilder loads these CSVs into a SQLite database
 * (data/links_index/links.db) indexed by both ref columns. This enables:
 *
 *   1. Chunk enrichment (Phase 2): each chunk gets up to 20 linked_refs pre-fetched
 *   2. Two-hop retrieval (agent): given a retrieved chunk, fetch related chunks by ref
 *
 * === Download strategy ===
 *
 * All 16 CSV files plus the table_of_contents.json are downloaded in parallel
 * using Vert.x WebClient. Individual files are large (~40MB each) so we
 * stream directly to disk via Buffer write rather than buffering in memory.
 */
public class LinksDownloader extends AbstractVerticle
{
    private static final Logger log = LoggerFactory.getLogger(LinksDownloader.class);
    private static final String LINKS_BASE_URL =
            "https://storage.googleapis.com/sefaria-export/links/";
    static final int LINKS_FILE_COUNT = 16;

    private final Path linksDir;
    private final Runnable onComplete;

    private WebClient webClient;

    public LinksDownloader(Path linksDir, Runnable onComplete)
    {
        this.linksDir = linksDir;
        this.onComplete = onComplete;
    }

    @Override
    public void start(Future<Void> startFuture)
    {
        WebClientOptions options = new WebClientOptions()
                .setMaxPoolSize(8)
                .setConnectTimeout(30000)
                .setIdleTimeout(300); // large files, generous timeout
        webClient = WebClient.create(vertx, options);

        vertx.executeBlocking(
            blocking ->
            {
                try
                {
                    Files.createDirectories(linksDir);
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
                    startFuture.fail(dirResult.cause());
                    return;
                }
                downloadAll(startFuture);
            }
        );
    }

    private void downloadAll(Future<Void> startFuture)
    {
        List<String[]> downloads = buildDownloadList();
        log.info("Downloading {} links/toc files...", downloads.size());

        List<Future> futures = new ArrayList<>();
        for (String[] pair : downloads)
        {
            futures.add(downloadFile(pair[0], pair[1]));
        }

        CompositeFuture.join(futures).setHandler(ar ->
        {
            log.info("Links download complete.");
            onComplete.run();
            startFuture.complete();
        });
    }

    private List<String[]> buildDownloadList()
    {
        List<String[]> list = new ArrayList<>();
        for (int i = 0; i < LINKS_FILE_COUNT; i++)
        {
            String name = "links" + i + ".csv";
            list.add(new String[]{LINKS_BASE_URL + name, name});
        }
        list.add(new String[]{LINKS_BASE_URL + "links_by_book.csv", "links_by_book.csv"});
        list.add(new String[]{
            "https://storage.googleapis.com/sefaria-export/table_of_contents.json",
            "table_of_contents.json"
        });
        return list;
    }

    private Future<Void> downloadFile(String url, String filename)
    {
        Future<Void> future = Future.future();
        Path dest = linksDir.resolve(filename);

        if (isNonEmpty(dest))
        {
            log.debug("Skipping existing: {}", filename);
            future.complete();
            return future;
        }

        log.info("Downloading {} ...", filename);

        webClient.getAbs(url)
                .timeout(300_000) // 5 minutes for large CSV files
                .send(ar ->
                {
                    if (ar.failed())
                    {
                        log.error("Failed to download {}: {}", filename, ar.cause().getMessage());
                        future.complete(); // don't abort all other downloads
                        return;
                    }

                    if (ar.result().statusCode() != 200)
                    {
                        log.error("HTTP {} for {}", ar.result().statusCode(), filename);
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
                                blocking.complete(null);
                            }
                            catch (Exception e)
                            {
                                blocking.fail(e);
                            }
                        },
                        writeResult ->
                        {
                            try
                            {
                                long sizeMb = Files.size(dest) / 1_048_576;
                                log.info("Downloaded {} ({} MB)", filename, sizeMb);
                            }
                            catch (Exception ignored) {}
                            future.complete();
                        }
                    );
                });

        return future;
    }

    /** Returns paths to all downloaded links CSV files. */
    public List<Path> getLinksCsvPaths()
    {
        List<Path> paths = new ArrayList<>();
        for (int i = 0; i < LINKS_FILE_COUNT; i++)
        {
            Path p = linksDir.resolve("links" + i + ".csv");
            if (Files.exists(p))
            {
                paths.add(p);
            }
        }
        return paths;
    }

    private boolean isNonEmpty(Path path)
    {
        try
        {
            return Files.exists(path) && Files.size(path) > 100;
        }
        catch (Exception e)
        {
            return false;
        }
    }
}
