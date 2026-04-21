package org.freememory.pipeline.agent;

import dev.langchain4j.model.embedding.EmbeddingModel;
import io.qdrant.client.QdrantClient;
import org.freememory.config.PipelineConfig.LocationConfig;
import org.freememory.pipeline.agent.tools.DateTimeTools;
import org.freememory.pipeline.agent.tools.HebrewCalendarTools;
import org.freememory.pipeline.agent.tools.SefariaLookupTools;
import org.freememory.pipeline.agent.tools.ZmanimTools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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

    /**
     * Semantic vector search of the Sefaria database — lets the LLM issue
     * targeted sub-queries to retrieve more sources on demand.
     * The LLM should call this multiple times with different search angles
     * for comprehensive, multi-source answers.
     */
    public static final String SEFARIA_SEARCH = "SEFARIA_SEARCH";

    // ------------------------------------------------------------------
    // Construction
    // ------------------------------------------------------------------

    private final QdrantClient   qdrant;
    private final EmbeddingModel embeddingModel;
    private final String         collectionName;
    private final LocationConfig location;

    public ToolRegistry(QdrantClient qdrant,
                        EmbeddingModel embeddingModel,
                        String collectionName,
                        LocationConfig location)
    {
        this.qdrant         = qdrant;
        this.embeddingModel = embeddingModel;
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

        // Deduplicate by class: SEFARIA_LOOKUP and SEFARIA_SEARCH both resolve to
        // SefariaLookupTools (which exposes both @Tool methods). Registering the
        // same class twice would create duplicate tool bindings in LangChain4j.
        Set<Class<?>> registered = new HashSet<>();

        for (String name : toolNames)
        {
            Object tool = instantiate(name.toUpperCase());
            if (tool == null)
            {
                log.warn("  Unknown built-in tool name \"{}\" — skipping. "
                        + "Check the spelling or add it to ToolRegistry.", name);
                continue;
            }
            if (registered.add(tool.getClass()))
            {
                tools.add(tool);
                log.debug("  Registered built-in tool: {}", name);
            }
            else
            {
                log.debug("  Skipping duplicate tool class for \"{}\": {} already registered.",
                        name, tool.getClass().getSimpleName());
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
            // Both SEFARIA_LOOKUP and SEFARIA_SEARCH are methods on the same class.
            // Return the same instance so only one object is registered per leaf.
            case SEFARIA_LOOKUP,
                 SEFARIA_SEARCH  -> new SefariaLookupTools(qdrant, embeddingModel, collectionName);
            default              -> null;
        };
    }
}
