package org.freememory.pipeline.process;

import org.freememory.pipeline.download.LinksIndexBuilder;
import org.freememory.pipeline.download.LinksIndexBuilder.LinkedRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Enriches text chunks with cross-reference metadata from the links SQLite index.
 *
 * === What enrichment does ===
 *
 * After chunking, each TextChunk has its ref, text, and classification fields
 * set, but its linkedRefs/linkTypes fields are empty. This class fills those
 * fields by querying the links.db for all references connected to the chunk's ref.
 *
 * For example, enriching the chunk for "Genesis 1:1" might yield:
 *   linkedRefs: ["Bereishit Rabbah 1:1", "Rashi on Genesis 1:1", "John 1:1"]
 *   linkTypes:  ["commentary", "commentary", "quotation"]
 *
 * These pre-fetched links are stored directly in the Qdrant payload so the
 * agent can do "two-hop retrieval" without a runtime SQLite query.
 *
 * === Configuration ===
 *
 * MAX_LINKS_PER_CHUNK (default 20): How many linked refs to store per chunk.
 * We cap this to keep payload size reasonable. The most-connected refs in
 * the Talmud can have hundreds of links; we store only the first 20.
 *
 * === Halakha topic extraction ===
 *
 * For Mishneh Torah and Shulchan Arukh chunks, we extract the halakhic topic
 * from the text's title. For example:
 *   "Mishneh Torah, Prayer"  → halakhaTopic = "Prayer"
 *   "Shulchan Arukh, Orach Chayim" → halakhaTopic = "Orach Chayim"
 *
 * This is stored in the chunk metadata and used by the agent's Halakha router
 * to improve topic-specific filtering.
 *
 * === Threading ===
 *
 * The SQLite connection is not thread-safe. Each instance of ChunkEnricher
 * holds its own connection. In the ProcessScript Verticle, one ChunkEnricher
 * is created per worker thread. All calls to enrich() are blocking and must
 * be wrapped in vertx.executeBlocking().
 */
public class ChunkEnricher
{
    private static final Logger log = LoggerFactory.getLogger(ChunkEnricher.class);
    private static final int MAX_LINKS_PER_CHUNK = 20;

    private final Connection conn;

    /**
     * Create an enricher backed by the given links.db path.
     * The connection is kept open for the lifetime of this enricher.
     * Call close() when done.
     */
    public ChunkEnricher(java.nio.file.Path linksDbPath) throws SQLException
    {
        String jdbcUrl = "jdbc:sqlite:" + linksDbPath.toAbsolutePath();
        this.conn = DriverManager.getConnection(jdbcUrl);
        // Read-only pragmas for performance
        try (java.sql.Statement stmt = conn.createStatement())
        {
            stmt.execute("PRAGMA query_only=ON");
            stmt.execute("PRAGMA cache_size=10000");
        }
    }

    /**
     * Enrich a single chunk with linked refs and halakha topic.
     * This is a blocking method — call from executeBlocking in Vert.x.
     */
    public void enrich(TextChunk chunk)
    {
        enrichLinks(chunk);
        enrichHalakhaTopic(chunk);
    }

    /** Enrich a list of chunks. Stops early if the connection is closed. */
    public void enrichAll(List<TextChunk> chunks)
    {
        for (TextChunk chunk : chunks)
        {
            try
            {
                enrich(chunk);
            }
            catch (Exception e)
            {
                log.warn("Failed to enrich chunk {}: {}", chunk.getRef(), e.getMessage());
            }
        }
    }

    public void close()
    {
        try
        {
            if (conn != null && !conn.isClosed())
            {
                conn.close();
            }
        }
        catch (SQLException e)
        {
            log.warn("Error closing links DB connection: {}", e.getMessage());
        }
    }

    // ------------------------------------------------------------------
    // Private enrichment methods
    // ------------------------------------------------------------------

    private void enrichLinks(TextChunk chunk)
    {
        String ref = chunk.getRef();
        if (ref == null || ref.isBlank())
        {
            return;
        }

        // Remove range suffixes for lookup: "Genesis 1:1-8" → "Genesis 1:1"
        // (links are indexed by individual verse refs, not ranges)
        String lookupRef = stripRangeSuffix(ref);

        try
        {
            List<LinkedRef> linked = LinksIndexBuilder.getLinkedRefs(
                    conn, lookupRef, MAX_LINKS_PER_CHUNK);

            if (linked.isEmpty())
            {
                return;
            }

            List<String> refs = new ArrayList<>();
            List<String> types = new ArrayList<>();
            for (LinkedRef lr : linked)
            {
                refs.add(lr.ref);
                types.add(lr.connectionType != null ? lr.connectionType : "");
            }
            chunk.setLinkedRefs(refs);
            chunk.setLinkTypes(types);
        }
        catch (SQLException e)
        {
            log.debug("Links lookup failed for {}: {}", ref, e.getMessage());
        }
    }

    private void enrichHalakhaTopic(TextChunk chunk)
    {
        if (!"Halakhah".equals(chunk.getCategory()))
        {
            return;
        }

        String title = chunk.getTitle();
        if (title == null)
        {
            return;
        }

        // "Mishneh Torah, Prayer" → "Prayer"
        // "Shulchan Arukh, Orach Chayim" → "Orach Chayim"
        // "Arukh HaShulchan, Yoreh De'ah" → "Yoreh De'ah"
        int commaIdx = title.indexOf(", ");
        if (commaIdx >= 0)
        {
            chunk.setHalakhaTopic(title.substring(commaIdx + 2).trim());
        }
    }

    /**
     * Strip a range suffix so we can look up the base ref in the links index.
     * "Genesis 1:1-8"   → "Genesis 1:1"
     * "Shabbat 2a"      → "Shabbat 2a"  (no range, unchanged)
     * "Genesis 1:1 (part 2)" → "Genesis 1:1"
     */
    private static String stripRangeSuffix(String ref)
    {
        // Remove " (part N)" suffixes
        int parenIdx = ref.indexOf(" (part ");
        if (parenIdx >= 0)
        {
            ref = ref.substring(0, parenIdx);
        }

        // Remove range: "1:1-8" → "1:1"  or  "1:1-2:3" → "1:1"
        // Find the last ':' then look for '-' after it
        int lastColon = ref.lastIndexOf(':');
        if (lastColon >= 0)
        {
            int dashAfterColon = ref.indexOf('-', lastColon);
            if (dashAfterColon >= 0)
            {
                ref = ref.substring(0, dashAfterColon);
            }
        }

        return ref;
    }
}
