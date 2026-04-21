package org.freememory.pipeline.agent;

import io.qdrant.client.QdrantClient;
import org.freememory.config.PipelineConfig.LocationConfig;
import org.freememory.pipeline.agent.tools.DateTimeTools;
import org.freememory.pipeline.agent.tools.HebrewCalendarTools;
import org.freememory.pipeline.agent.tools.SefariaLookupTools;
import org.freememory.pipeline.agent.tools.ZmanimTools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Maps built-in tool name strings to tool provider instances.
 *
 * Tool names are listed in the agent config JSON under each leaf's {@code tools}
 * array (per-leaf) or under {@code agent.globalTools} (given to every leaf).
 *
 * === Adding a new built-in tool ===
 *
 * 1. Create a {@code *Tools.java} class in {@code org.freememory.pipeline.agent.tools}
 *    with one or more {@code @Tool}-annotated methods.
 * 2. Add a constant for the name (e.g. {@code "MY_TOOL"}) and a case in
 *    {@link #instantiate}.
 * 3. Reference the name in {@code agent.globalTools} (if cross-cutting) or in
 *    the leaf's {@code tools} array (if domain-specific).
 *
 * === External tools via MCP ===
 *
 * MCP-based tools are configured separately under each leaf's {@code mcpServers}
 * array or {@code agent.globalMcpServers} and are wired in by {@link AgentTree}
 * using LangChain4j's {@code McpToolProvider}.  They do not appear here.
 */
public class ToolRegistry
{
    private static final Logger log = LoggerFactory.getLogger(ToolRegistry.class);

    // ------------------------------------------------------------------
    // Built-in tool names
    // ------------------------------------------------------------------

    /**
     * Current date/time and default location — a global cross-cutting tool.
     * Provides getCurrentDateTime() and getDefaultLocation() so any agent
     * can ground itself in real time before making calendar or zmanim queries.
     */
    public static final String DATE_TIME = "DATE_TIME";

    /** Halachic times (zmanim) via the Hebcal API. */
    public static final String ZMANIM = "ZMANIM";

    /** Hebrew calendar: parasha, holidays, date conversion via Hebcal. */
    public static final String HEBREW_CALENDAR = "HEBREW_CALENDAR";

    /** Direct Sefaria text lookup by ref string (Qdrant point fetch). */
    public static final String SEFARIA_LOOKUP = "SEFARIA_LOOKUP";

    // ------------------------------------------------------------------
    // Construction
    // ------------------------------------------------------------------

    private final QdrantClient   qdrant;
    private final String         collectionName;
    private final LocationConfig location;

    public ToolRegistry(QdrantClient qdrant, String collectionName, LocationConfig location)
    {
        this.qdrant         = qdrant;
        this.collectionName = collectionName;
        this.location       = location != null ? location : new LocationConfig();
    }

    // ------------------------------------------------------------------
    // Resolution
    // ------------------------------------------------------------------

    /**
     * Build tool provider instances for the given list of tool name strings.
     * Unknown names are warned and skipped rather than throwing, so a config
     * typo does not abort the entire agent startup.
     */
    public List<Object> resolve(List<String> toolNames)
    {
        List<Object> tools = new ArrayList<>();
        if (toolNames == null || toolNames.isEmpty())
        {
            return tools;
        }

        for (String name : toolNames)
        {
            Object tool = instantiate(name.toUpperCase());
            if (tool != null)
            {
                tools.add(tool);
                log.debug("  Registered built-in tool: {}", name);
            }
            else
            {
                log.warn("  Unknown built-in tool name \"{}\" — skipping. "
                        + "Check the spelling or add it to ToolRegistry.", name);
            }
        }
        return tools;
    }

    // ------------------------------------------------------------------
    // Private
    // ------------------------------------------------------------------

    private Object instantiate(String name)
    {
        return switch (name)
        {
            case DATE_TIME       -> new DateTimeTools(location.getCity(), location.getTimezone());
            case ZMANIM          -> new ZmanimTools();
            case HEBREW_CALENDAR -> new HebrewCalendarTools();
            case SEFARIA_LOOKUP  -> new SefariaLookupTools(qdrant, collectionName);
            default              -> null;
        };
    }
}
