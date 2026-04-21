package org.freememory.pipeline.agent;

import dev.langchain4j.mcp.McpToolProvider;
import dev.langchain4j.mcp.client.DefaultMcpClient;
import dev.langchain4j.mcp.client.McpClient;
import dev.langchain4j.mcp.client.transport.McpTransport;
import dev.langchain4j.mcp.client.transport.http.HttpMcpTransport;
import dev.langchain4j.mcp.client.transport.stdio.StdioMcpTransport;
import dev.langchain4j.model.chat.ChatModel;
import dev.langchain4j.model.embedding.EmbeddingModel;
import io.qdrant.client.QdrantClient;
import org.freememory.config.PipelineConfig.AgentConfig;
import org.freememory.config.PipelineConfig.AgentNodeConfig;
import org.freememory.config.PipelineConfig.McpServerConfig;
import org.freememory.config.PipelineConfig.ModelConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Builds the runtime agent tree from the JSON configuration.
 *
 * === How to add a new agent ===
 *
 * Edit your config JSON — no code changes needed:
 *
 *   1. Add a new node object to the parent router's {@code children} array.
 *   2. Set {@code type}, {@code name}, {@code description}.
 *   3. For a leaf: add {@code promptFile}, {@code retrieval}, and optionally
 *      {@code tools} / {@code mcpServers}.
 *   4. Add the corresponding {@code prompts/<name>.txt} classpath resource.
 *
 * === Built-in tools ===
 *
 * Tool name strings (e.g. {@code "ZMANIM"}) are resolved by {@link ToolRegistry}.
 *
 * === MCP tools ===
 *
 * Each entry in a leaf's {@code mcpServers} array is connected at startup:
 *   - {@code url} present → HTTP/SSE transport ({@link HttpMcpTransport})
 *   - {@code command} present → stdio transport ({@link StdioMcpTransport})
 *
 * All MCP clients for a leaf are combined into one {@link McpToolProvider} and
 * registered on the leaf's {@link AiServices} builder.
 */
public class AgentTree
{
    private static final Logger log = LoggerFactory.getLogger(AgentTree.class);

    private final AgentConfig    agentConfig;
    private final QdrantClient   qdrant;
    private final EmbeddingModel embeddingModel;
    private final ToolRegistry   toolRegistry;

    public AgentTree(AgentConfig agentConfig,
                     QdrantClient qdrant,
                     EmbeddingModel embeddingModel)
    {
        this.agentConfig    = agentConfig;
        this.qdrant         = qdrant;
        this.embeddingModel = embeddingModel;
        this.toolRegistry   = new ToolRegistry(
                qdrant, agentConfig.getCollectionName(), agentConfig.getDefaultLocation());
    }

    // ------------------------------------------------------------------
    // Public API
    // ------------------------------------------------------------------

    /**
     * Build and return the root {@link AgentNode} from the config tree.
     *
     * @throws IllegalStateException if the config tree is null or malformed
     */
    public AgentNode build()
    {
        AgentNodeConfig treeConfig = agentConfig.getTree();
        if (treeConfig == null)
        {
            throw new IllegalStateException(
                    "agent.tree is not defined in the config file. "
                    + "Add a 'tree' object with type='router' or type='leaf'.");
        }

        log.info("Building agent tree...");
        AgentNode root = buildNode(treeConfig, agentConfig.getDefaultModel());
        log.info("Agent tree ready. Root: '{}'", root.getName());
        return root;
    }

    // ------------------------------------------------------------------
    // Recursive build
    // ------------------------------------------------------------------

    private AgentNode buildNode(AgentNodeConfig cfg, ModelConfig inheritedModel)
    {
        // Resolve the effective model: per-node override, else inherited default
        ModelConfig effectiveModel = (cfg.getModel() != null)
                ? cfg.getModel() : inheritedModel;

        if (cfg.isRouter())
        {
            return buildRouter(cfg, effectiveModel);
        }
        else if (cfg.isLeaf())
        {
            return buildLeaf(cfg, effectiveModel);
        }
        else
        {
            throw new IllegalArgumentException(
                    "Agent node '" + cfg.getName()
                    + "' has unknown type '" + cfg.getType()
                    + "'. Must be 'router' or 'leaf'.");
        }
    }

    private RouterNode buildRouter(AgentNodeConfig cfg, ModelConfig effectiveModel)
    {
        log.info("  Router: '{}'", cfg.getName());

        // Routers use the router model (cheap classification model)
        ChatModel routerModel = ModelFactory.create(
                agentConfig.getRouterModel(), agentConfig);

        List<AgentNode> children = new ArrayList<>();
        for (AgentNodeConfig childCfg : cfg.getChildren())
        {
            // Children inherit the leaf default model, not the router model
            children.add(buildNode(childCfg, agentConfig.getDefaultModel()));
        }

        return new RouterNode(cfg.getName(), cfg.getDescription(),
                routerModel, children);
    }

    private LeafNode buildLeaf(AgentNodeConfig cfg, ModelConfig effectiveModel)
    {
        log.info("  Leaf:   '{}'", cfg.getName());

        ChatModel model = ModelFactory.create(effectiveModel, agentConfig);

        SefariaRetriever retriever = new SefariaRetriever(
                qdrant, embeddingModel,
                agentConfig.getCollectionName(),
                cfg.getRetrieval(),
                agentConfig.linksDbPath());

        // Merge global tools + leaf-specific tools (global first, then per-leaf extras)
        List<String> allToolNames = new ArrayList<>(agentConfig.getGlobalTools());
        if (cfg.getTools() != null)
        {
            for (String t : cfg.getTools())
            {
                if (!allToolNames.contains(t))
                {
                    allToolNames.add(t);
                }
            }
        }
        List<Object> builtInTools = toolRegistry.resolve(allToolNames);

        // Merge global MCP servers + leaf-specific MCP servers
        List<Object> mcpTools = buildMcpProviders(cfg);

        String systemPrompt = loadPrompt(cfg.getPromptFile(), cfg.getName());

        return new LeafNode(
                cfg.getName(),
                cfg.getDescription(),
                systemPrompt,
                model,
                retriever,
                builtInTools,
                mcpTools,
                agentConfig.getChatMemorySize()
        );
    }

    // ------------------------------------------------------------------
    // MCP tool providers
    // ------------------------------------------------------------------

    private List<Object> buildMcpProviders(AgentNodeConfig cfg)
    {
        // Merge global MCP servers + leaf-specific MCP servers
        List<McpServerConfig> serverCfgs = new ArrayList<>();
        if (agentConfig.getGlobalMcpServers() != null)
        {
            serverCfgs.addAll(agentConfig.getGlobalMcpServers());
        }
        if (cfg.getMcpServers() != null)
        {
            serverCfgs.addAll(cfg.getMcpServers());
        }

        if (serverCfgs.isEmpty())
        {
            return List.of();
        }

        List<McpClient> clients = new ArrayList<>();
        for (McpServerConfig server : serverCfgs)
        {
            try
            {
                McpTransport transport = buildMcpTransport(server);
                McpClient client = DefaultMcpClient.builder()
                        .key(server.getKey())
                        .transport(transport)
                        .build();
                clients.add(client);
                log.info("    MCP server connected: {} ({})",
                        server.getKey(), server.isHttp() ? server.getUrl() : "stdio");
            }
            catch (Exception e)
            {
                log.error("    Failed to connect MCP server '{}': {}",
                        server.getKey(), e.getMessage());
            }
        }

        if (clients.isEmpty())
        {
            return List.of();
        }

        McpToolProvider provider = McpToolProvider.builder()
                .mcpClients(clients)
                .build();

        return List.of(provider);
    }

    private static McpTransport buildMcpTransport(McpServerConfig server) throws Exception
    {
        if (server.isHttp())
        {
            return HttpMcpTransport.builder()
                    .sseUrl(server.getUrl())
                    .build();
        }
        else if (server.isStdio())
        {
            return StdioMcpTransport.builder()
                    .command(server.getCommand())
                    .logEvents(false)
                    .build();
        }
        else
        {
            throw new IllegalArgumentException(
                    "MCP server '" + server.getKey()
                    + "' must have either 'url' or 'command' set.");
        }
    }

    // ------------------------------------------------------------------
    // Prompt loading
    // ------------------------------------------------------------------

    /**
     * Load the system prompt for a leaf from a classpath resource.
     *
     * If {@code promptFile} is null / blank, or the resource is not found,
     * a minimal default prompt is returned so the agent still functions.
     */
    private static String loadPrompt(String promptFile, String agentName)
    {
        if (promptFile == null || promptFile.isBlank())
        {
            log.warn("No promptFile configured for leaf '{}'. Using default prompt.", agentName);
            return defaultPrompt(agentName);
        }

        // Normalise path: strip leading slash so getResourceAsStream works with
        // both "prompts/foo.txt" and "/prompts/foo.txt".
        String path = promptFile.startsWith("/") ? promptFile.substring(1) : promptFile;

        try (InputStream is = AgentTree.class.getClassLoader().getResourceAsStream(path))
        {
            if (is == null)
            {
                log.warn("Prompt resource '{}' not found for leaf '{}'. "
                        + "Using default prompt.", path, agentName);
                return defaultPrompt(agentName);
            }
            return new String(is.readAllBytes(), StandardCharsets.UTF_8);
        }
        catch (IOException e)
        {
            log.error("Failed to read prompt resource '{}': {}", path, e.getMessage());
            return defaultPrompt(agentName);
        }
    }

    private static String defaultPrompt(String agentName)
    {
        return "You are " + agentName + ", a Jewish learning assistant. "
             + "Answer questions accurately and cite your sources by their "
             + "Sefaria reference (e.g. Shabbat 2a, Mishneh Torah Prayer 2:4). "
             + "Use the provided source texts as your primary evidence.";
    }
}
