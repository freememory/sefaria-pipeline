package org.freememory.pipeline.download;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Builds a SQLite database from the Sefaria links CSV files.
 *
 * === Why SQLite? ===
 *
 * The links data (~16 CSV files, ~650MB) contains millions of cross-references.
 * Storing them in SQLite with indexes on both ref columns gives sub-millisecond
 * lookups at runtime — far faster than scanning CSVs on every query.
 *
 * === Schema ===
 *
 *   CREATE TABLE links (
 *     ref1             TEXT NOT NULL,
 *     ref2             TEXT NOT NULL,
 *     connection_type  TEXT,
 *     category1        TEXT,
 *     category2        TEXT
 *   );
 *   CREATE INDEX idx_ref1 ON links(ref1);
 *   CREATE INDEX idx_ref2 ON links(ref2);
 *
 * === CSV Format ===
 *
 * Input files use the header:
 *   Citation 1, Citation 2, Connection Type, Text 1, Text 2, Category 1, Category 2
 *
 * Citation 1/2 are the canonical Sefaria Ref strings (e.g., "Genesis 1:1").
 * Connection Type values: commentary, quotation, reference, parallel, targum, etc.
 *
 * === Usage ===
 *
 * This is a blocking operation and must be called from a Vert.x executeBlocking
 * block or from a non-event-loop thread. It takes approximately 10 minutes to
 * build the full index from all 16 CSV files.
 *
 * Call build(false) to build if not present, or build(true) to force a rebuild.
 *
 * After building, use the static query methods to look up links by ref string.
 */
public class LinksIndexBuilder
{
    private static final Logger log = LoggerFactory.getLogger(LinksIndexBuilder.class);
    private static final int BATCH_SIZE = 10_000;

    private final Path linksDir;
    private final Path dbPath;

    public LinksIndexBuilder(Path linksDir, Path dbPath)
    {
        this.linksDir = linksDir;
        this.dbPath = dbPath;
    }

    /**
     * Build (or rebuild) the SQLite index from downloaded CSV files.
     *
     * This is a blocking method — call from executeBlocking in Vert.x.
     *
     * @param force if true, delete and rebuild even if links.db already exists
     */
    public void build(boolean force) throws IOException, SQLException
    {
        if (force && Files.exists(dbPath))
        {
            Files.delete(dbPath);
            log.info("Deleted existing links.db for rebuild");
        }

        if (Files.exists(dbPath))
        {
            log.info("links.db already exists at {}. Pass force=true to rebuild.", dbPath);
            return;
        }

        Files.createDirectories(dbPath.getParent());

        List<Path> csvFiles = new ArrayList<>();
        for (int i = 0; i < LinksDownloader.LINKS_FILE_COUNT; i++)
        {
            Path p = linksDir.resolve("links" + i + ".csv");
            if (Files.exists(p))
            {
                csvFiles.add(p);
            }
        }

        if (csvFiles.isEmpty())
        {
            throw new IOException("No links CSV files found in " + linksDir
                    + ". Run DownloadScript first.");
        }

        log.info("Building links.db from {} CSV files...", csvFiles.size());

        String jdbcUrl = "jdbc:sqlite:" + dbPath.toAbsolutePath();
        try (Connection conn = DriverManager.getConnection(jdbcUrl))
        {
            createSchema(conn);
            long totalRows = 0;

            for (Path csvFile : csvFiles)
            {
                long rows = importCsv(conn, csvFile);
                totalRows += rows;
                log.info("Imported {} rows from {}", rows, csvFile.getFileName());
            }

            log.info("Creating indexes...");
            createIndexes(conn);
            log.info("Links index built: {} total rows → {}", totalRows, dbPath);
        }
    }

    // ------------------------------------------------------------------
    // Static query helpers (use from agent and enricher)
    // ------------------------------------------------------------------

    /**
     * Get all refs linked to the given ref (searches both directions).
     * Returns up to `limit` results as (otherRef, connectionType) pairs.
     */
    public static List<LinkedRef> getLinkedRefs(Connection conn, String ref, int limit)
            throws SQLException
    {
        String sql = "SELECT CASE WHEN ref1 = ? THEN ref2 ELSE ref1 END AS other_ref, "
                   + "connection_type "
                   + "FROM links WHERE ref1 = ? OR ref2 = ? LIMIT ?";

        List<LinkedRef> results = new ArrayList<>();
        try (PreparedStatement stmt = conn.prepareStatement(sql))
        {
            stmt.setString(1, ref);
            stmt.setString(2, ref);
            stmt.setString(3, ref);
            stmt.setInt(4, limit);
            ResultSet rs = stmt.executeQuery();
            while (rs.next())
            {
                results.add(new LinkedRef(
                        rs.getString("other_ref"),
                        rs.getString("connection_type")));
            }
        }
        return results;
    }

    // ------------------------------------------------------------------
    // Private helpers
    // ------------------------------------------------------------------

    private void createSchema(Connection conn) throws SQLException
    {
        try (Statement stmt = conn.createStatement())
        {
            stmt.execute("PRAGMA journal_mode=WAL");
            stmt.execute("PRAGMA synchronous=NORMAL");
            stmt.execute(
                "CREATE TABLE IF NOT EXISTS links ("
              + "  ref1             TEXT NOT NULL,"
              + "  ref2             TEXT NOT NULL,"
              + "  connection_type  TEXT,"
              + "  category1        TEXT,"
              + "  category2        TEXT"
              + ")"
            );
        }
    }

    private void createIndexes(Connection conn) throws SQLException
    {
        try (Statement stmt = conn.createStatement())
        {
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_ref1 ON links(ref1)");
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_ref2 ON links(ref2)");
        }
    }

    private long importCsv(Connection conn, Path csvFile) throws IOException, SQLException
    {
        CSVFormat format = CSVFormat.DEFAULT.builder()
                .setHeader()
                .setSkipHeaderRecord(true)
                .setIgnoreHeaderCase(true)
                .setTrim(true)
                .setIgnoreEmptyLines(true)
                .build();

        String insertSql =
            "INSERT INTO links(ref1, ref2, connection_type, category1, category2) "
          + "VALUES(?,?,?,?,?)";

        long count = 0;
        conn.setAutoCommit(false);

        try (BufferedReader reader = Files.newBufferedReader(csvFile, StandardCharsets.UTF_8);
             CSVParser parser = new CSVParser(reader, format);
             PreparedStatement stmt = conn.prepareStatement(insertSql))
        {
            for (CSVRecord record : parser)
            {
                try
                {
                    String ref1 = safeGet(record, "Citation 1", 0);
                    String ref2 = safeGet(record, "Citation 2", 1);
                    if (ref1 == null || ref1.isBlank() || ref2 == null || ref2.isBlank())
                    {
                        continue;
                    }

                    stmt.setString(1, ref1);
                    stmt.setString(2, ref2);
                    stmt.setString(3, safeGet(record, "Connection Type", 2));
                    stmt.setString(4, safeGet(record, "Category 1", 5));
                    stmt.setString(5, safeGet(record, "Category 2", 6));
                    stmt.addBatch();
                    count++;

                    if (count % BATCH_SIZE == 0)
                    {
                        stmt.executeBatch();
                        conn.commit();
                    }
                }
                catch (Exception ignored)
                {
                    // Skip malformed rows silently
                }
            }

            stmt.executeBatch();
            conn.commit();
        }

        conn.setAutoCommit(true);
        return count;
    }

    private String safeGet(CSVRecord record, String header, int fallbackIndex)
    {
        try
        {
            if (record.isMapped(header))
            {
                return record.get(header);
            }
            if (record.size() > fallbackIndex)
            {
                return record.get(fallbackIndex);
            }
        }
        catch (Exception ignored) {}
        return null;
    }

    // ------------------------------------------------------------------
    // Data records
    // ------------------------------------------------------------------

    /** A ref linked to a given source ref, with the type of connection. */
    public static class LinkedRef
    {
        public final String ref;
        public final String connectionType;

        public LinkedRef(String ref, String connectionType)
        {
            this.ref = ref;
            this.connectionType = connectionType;
        }
    }
}
