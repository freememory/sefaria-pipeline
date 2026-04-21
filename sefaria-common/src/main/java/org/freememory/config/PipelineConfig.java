package org.freememory.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.nio.file.Path;
import java.util.Set;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Root configuration object for the entire Sefaria pipeline.
 *
 * Loaded from a single JSON file (e.g. config/pipeline-p0.json) by each
 * script. Each script reads only the section relevant to its phase.
 *
 * Example usage in a script:
 *
 *   PipelineConfig config = ConfigLoader.load(args);
 *   PipelineConfig.DownloadConfig dl = config.getDownload();
 *
 * See src/main/resources/config/ for ready-to-use example config files.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class PipelineConfig
{
    @JsonProperty("download")
    private DownloadConfig download = new DownloadConfig();

    @JsonProperty("process")
    private ProcessConfig process = new ProcessConfig();

    @JsonProperty("embed")
    private EmbedConfig embed = new EmbedConfig();

    @JsonProperty("agent")
    private AgentConfig agent = new AgentConfig();

    public DownloadConfig getDownload() { return download; }
    public ProcessConfig getProcess()   { return process; }
    public EmbedConfig getEmbed()       { return embed; }
    public AgentConfig getAgent()       { return agent; }

    // ------------------------------------------------------------------
    // Phase 1 — Download
    // ------------------------------------------------------------------

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class DownloadConfig
    {
        /** Directory where downloaded text JSON files are written. */
        @JsonProperty("outputDir")
        private String outputDir = "data/raw";

        /** Directory where schema JSON files are written. */
        @JsonProperty("schemasDir")
        private String schemasDir = "data/schemas";

        /** Directory where links CSV files are written. */
        @JsonProperty("linksDir")
        private String linksDir = "data/links";

        /** Directory where the SQLite links index is written. */
        @JsonProperty("linksIndexDir")
        private String linksIndexDir = "data/links_index";

        /**
         * Path to books.json. If null, the script will look for it in the
         * sibling Sefaria-Export directory.
         */
        @JsonProperty("booksJson")
        private String booksJson = null;

        /** Number of parallel HTTP connections for downloading. */
        @JsonProperty("concurrency")
        private int concurrency = 20;

        /**
         * Priority tiers to download. Valid values: P0, P1, P2, P3.
         * An empty list means download all tiers.
         *
         * Tiers:
         *   P0 — Tanakh, Mishnah, Talmud, Halakhah  (core sources)
         *   P1 — Responsa, Tosefta, Midrash, Liturgy
         *   P2 — Jewish Thought, Kabbalah, Chasidut, Musar, Second Temple
         *   P3 — All commentaries (Rashi, Ramban, etc.)
         */
        @JsonProperty("priorities")
        private List<Priority> priorities = List.of();

        /** If true, skip downloading the links CSV files (~650MB). */
        @JsonProperty("skipLinks")
        private boolean skipLinks = false;

        /** If true, skip downloading schema files. */
        @JsonProperty("skipSchemas")
        private boolean skipSchemas = false;

        /** If true, build the SQLite links index after downloading CSVs. */
        @JsonProperty("buildIndex")
        private boolean buildIndex = true;

        /** If true, delete and rebuild the links index even if it already exists. */
        @JsonProperty("forceIndex")
        private boolean forceIndex = false;

        // --- Path accessors (convert String → Path lazily) ---

        public Path outputDirPath()     { return Path.of(outputDir); }
        public Path schemasDirPath()    { return Path.of(schemasDir); }
        public Path linksDirPath()      { return Path.of(linksDir); }
        public Path linksIndexDirPath() { return Path.of(linksIndexDir); }
        public Path linksDbPath()       { return Path.of(linksIndexDir).resolve("links.db"); }
        public Path booksJsonPath()     { return booksJson != null ? Path.of(booksJson) : null; }

        public int getConcurrency()       { return concurrency; }
        public List<Priority> getPriorities() { return priorities; }
        public boolean isSkipLinks()      { return skipLinks; }
        public boolean isSkipSchemas()    { return skipSchemas; }
        public boolean isBuildIndex()     { return buildIndex; }
        public boolean isForceIndex()     { return forceIndex; }

        // ------------------------------------------------------------------
        // Priority tiers
        //
        // Moved here from BooksJsonCatalog so the config model has no
        // dependency on the download implementation classes.
        // ------------------------------------------------------------------

        /**
         * Download priority tier.
         * <ul>
         *   <li>P0 — Tanakh, Mishnah, Talmud, Halakhah (core sources)</li>
         *   <li>P1 — Responsa, Tosefta, Midrash, Liturgy</li>
         *   <li>P2 — Jewish Thought, Kabbalah, Chasidut, Musar, Second Temple</li>
         *   <li>P3 — All commentaries (Rashi, Ramban, etc.)</li>
         * </ul>
         */
        public enum Priority
        {
            P0(Set.of("Tanakh", "Mishnah", "Talmud", "Halakhah")),
            P1(Set.of("Responsa", "Tosefta", "Midrash", "Liturgy")),
            P2(Set.of("Jewish Thought", "Kabbalah", "Chasidut", "Musar", "Second Temple")),
            P3(Set.of()); // Everything else (commentaries etc.)

            private final Set<String> cats;
            Priority(Set<String> cats) { this.cats = cats; }
            public Set<String> categories() { return cats; }
        }
    }

    // ------------------------------------------------------------------
    // Phase 2 — Process
    // ------------------------------------------------------------------

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ProcessConfig
    {
        /** Directory containing the downloaded merged.json files. */
        @JsonProperty("inputDir")
        private String inputDir = "data/raw";

        /** Directory containing the downloaded schema JSON files. */
        @JsonProperty("schemasDir")
        private String schemasDir = "data/schemas";

        /** Path to the SQLite links index built in Phase 1. */
        @JsonProperty("linksDb")
        private String linksDb = "data/links_index/links.db";

        /** Directory where JSONL chunk files are written. */
        @JsonProperty("outputDir")
        private String outputDir = "data/processed";

        public Path inputDirPath()   { return Path.of(inputDir); }
        public Path schemasDirPath() { return Path.of(schemasDir); }
        public Path linksDbPath()    { return Path.of(linksDb); }
        public Path outputDirPath()  { return Path.of(outputDir); }
    }

    // ------------------------------------------------------------------
    // Phase 3 — Embed + Ingest
    // ------------------------------------------------------------------

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class EmbedConfig
    {
        /** Directory containing JSONL files from Phase 2. */
        @JsonProperty("inputDir")
        private String inputDir = "data/processed";

        /**
         * OpenAI API key for the embedding model.
         * If null or blank, the OPENAI_API_KEY environment variable is used as a fallback.
         * Storing the key here is convenient for local development; never commit this
         * file to a public repository.
         */
        @JsonProperty("openAiApiKey")
        private String openAiApiKey = null;

        /**
         * OpenAI embedding model name.
         * text-embedding-3-small is multilingual (Hebrew, Aramaic, English).
         */
        @JsonProperty("embeddingModel")
        private String embeddingModel = "text-embedding-3-small";

        /**
         * Maximum number of chunks per OpenAI embedding call.
         * Acts as a secondary cap alongside maxBatchTokens.
         */
        @JsonProperty("batchSize")
        private int batchSize = 100;

        /**
         * Maximum total text characters per OpenAI embedding call.
         *
         * OpenAI's hard limit is 300,000 actual tokens per request. For Hebrew
         * text with nikkud (vowel-pointing diacritics), each Unicode code point
         * often becomes its own BPE token, so String.length() correlates directly
         * with actual token count — making it a much more reliable budget metric
         * than wordCount × 1.3, which can underestimate by 4–5× for pointed text.
         *
         * Default of 200,000 characters gives a comfortable safety margin under
         * the 300,000 token limit even for the most token-dense Hebrew texts.
         */
        @JsonProperty("maxBatchChars")
        private int maxBatchChars = 200_000;

        /** Qdrant host (gRPC). */
        @JsonProperty("qdrantHost")
        private String qdrantHost = "localhost";

        /** Qdrant gRPC port. */
        @JsonProperty("qdrantPort")
        private int qdrantPort = 6334;

        /** Qdrant collection name. */
        @JsonProperty("collectionName")
        private String collectionName = "sefaria_texts";

        /**
         * If true, skip chunks whose chunk_id already exists in Qdrant.
         * Allows safe re-runs without re-embedding everything.
         */
        @JsonProperty("skipExisting")
        private boolean skipExisting = true;

        /**
         * Maximum milliseconds to sleep after a full-size batch (maxBatchChars).
         *
         * Smaller batches sleep proportionally less:
         *   sleep = (actualBatchChars / maxBatchChars) × batchDelayMs
         *
         * Default is 0 (adaptive mode): the pipeline runs at full speed and only
         * backs off when OpenAI returns a rate-limit error. The error message
         * includes the exact wait time ("Please try again in 2.3s") which
         * ChunkIngestor parses and sleeps for precisely, then retries.
         *
         * Set to a positive value (e.g. 12000) for proactive throttling instead,
         * which avoids all rate-limit warnings at the cost of slower throughput.
         */
        @JsonProperty("batchDelayMs")
        private int batchDelayMs = 0;

        public Path inputDirPath()        { return Path.of(inputDir); }
        public String getOpenAiApiKey()   { return openAiApiKey; }
        public String getEmbeddingModel() { return embeddingModel; }
        public int getBatchSize()         { return batchSize; }
        public int getMaxBatchChars()     { return maxBatchChars; }
        public int getBatchDelayMs()      { return batchDelayMs; }
        public String getQdrantHost()     { return qdrantHost; }
        public int getQdrantPort()        { return qdrantPort; }
        public String getCollectionName() { return collectionName; }
        public boolean isSkipExisting()   { return skipExisting; }
    }

    // ------------------------------------------------------------------
    // Phase 4 — Agent
    // ------------------------------------------------------------------

    /**
     * Identifies a specific language model at a specific provider.
     *
     * Usage in config JSON:
     *   { "provider": "anthropic", "modelId": "claude-sonnet-4-6" }
     *
     * Supported providers: anthropic, openai, google, mistral, ollama
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ModelConfig
    {
        @JsonProperty("provider")
        private String provider = "anthropic";

        @JsonProperty("modelId")
        private String modelId = "claude-sonnet-4-6";

        /**
         * Maximum tokens in the model's response.
         * Defaults to 4096 — enough for long source-rich answers.
         * Set to 8096 for very detailed multi-source responses.
         * The router model ignores this (its responses are always short).
         */
        @JsonProperty("maxTokens")
        private int maxTokens = 4096;

        public String getProvider()  { return provider; }
        public String getModelId()   { return modelId; }
        public int    getMaxTokens() { return maxTokens; }
    }

    /**
     * API credentials and connection settings for one model provider.
     *
     * If apiKey is null or blank the corresponding environment variable is used:
     *   anthropic → ANTHROPIC_API_KEY
     *   openai    → OPENAI_API_KEY
     *   google    → GOOGLE_API_KEY
     *   mistral   → MISTRAL_API_KEY
     *
     * baseUrl is only used for Ollama (default: http://localhost:11434).
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ProviderConfig
    {
        @JsonProperty("apiKey")
        private String apiKey;

        @JsonProperty("baseUrl")
        private String baseUrl;

        public String getApiKey()  { return apiKey; }
        public String getBaseUrl() { return baseUrl; }
    }

    /**
     * Qdrant retrieval parameters for one leaf agent.
     *
     * categories  — if non-empty, results are filtered to these top-level categories
     *               (e.g. ["Halakhah", "Talmud", "Mishnah"]). Empty = no filter.
     * topicFilter — optional keyword matched against the "title" payload field.
     *               Use to narrow results to a specific tractate or book.
     * language    — "en", "he", or null / omit for both.
     * topK        — number of candidate vectors to retrieve from Qdrant.
     * primaryOnly — if true, only primary texts (not commentaries) are returned.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class RetrievalConfig
    {
        @JsonProperty("categories")
        private List<String> categories = List.of();

        @JsonProperty("topicFilter")
        private String topicFilter;

        @JsonProperty("language")
        private String language;

        @JsonProperty("topK")
        private int topK = 20;

        @JsonProperty("primaryOnly")
        private boolean primaryOnly = false;

        public List<String> getCategories() { return categories; }
        public String getTopicFilter()      { return topicFilter; }
        public String getLanguage()         { return language; }
        public int getTopK()                { return topK; }
        public boolean isPrimaryOnly()      { return primaryOnly; }
    }

    /**
     * One external MCP (Model Context Protocol) server to connect to.
     *
     * Exactly one of "url" (HTTP/SSE transport) or "command" (stdio transport)
     * must be set.
     *
     * Examples:
     *   HTTP:  { "key": "hebcal", "url": "http://localhost:3001/mcp" }
     *   Stdio: { "key": "fs", "command": ["npx", "@modelcontextprotocol/server-filesystem", "/data"] }
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class McpServerConfig
    {
        /** Unique identifier for this MCP connection (used in logs). */
        @JsonProperty("key")
        private String key;

        /** HTTP/SSE endpoint — set this OR command, not both. */
        @JsonProperty("url")
        private String url;

        /**
         * Stdio command — the process is launched as a child of the JVM.
         * Example: ["npx", "@modelcontextprotocol/server-everything"]
         */
        @JsonProperty("command")
        private List<String> command;

        public String       getKey()     { return key; }
        public String       getUrl()     { return url; }
        public List<String> getCommand() { return command; }
        public boolean      isHttp()     { return url != null && !url.isBlank(); }
        public boolean      isStdio()    { return command != null && !command.isEmpty(); }
    }

    /**
     * One node in the agent tree.  Can be a router (has children) or a leaf (has
     * promptFile + retrieval config + tools + optional MCP servers).
     *
     * type        — "router" or "leaf"
     * model       — optional model override; inherits from parent/default if absent
     * children    — router only: the sub-agents this node can delegate to
     * promptFile  — leaf only: classpath resource path, e.g. "prompts/halakha.txt"
     * tools       — leaf only: built-in tool names ["ZMANIM", "HEBREW_CALENDAR",
     *               "SEFARIA_LOOKUP"].  New tools are registered in ToolRegistry.
     * mcpServers  — leaf only: external MCP servers whose tools are exposed to this agent
     * retrieval   — leaf only: Qdrant filter + topK settings
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class AgentNodeConfig
    {
        @JsonProperty("name")
        private String name;

        @JsonProperty("description")
        private String description;

        @JsonProperty("type")
        private String type;

        @JsonProperty("model")
        private ModelConfig model;

        @JsonProperty("children")
        private List<AgentNodeConfig> children = List.of();

        @JsonProperty("promptFile")
        private String promptFile;

        @JsonProperty("tools")
        private List<String> tools = List.of();

        @JsonProperty("mcpServers")
        private List<McpServerConfig> mcpServers = List.of();

        @JsonProperty("retrieval")
        private RetrievalConfig retrieval = new RetrievalConfig();

        public String               getName()        { return name; }
        public String               getDescription() { return description; }
        public String               getType()        { return type; }
        public ModelConfig          getModel()       { return model; }
        public List<AgentNodeConfig> getChildren()   { return children; }
        public String               getPromptFile()  { return promptFile; }
        public List<String>         getTools()       { return tools; }
        public List<McpServerConfig> getMcpServers() { return mcpServers; }
        public RetrievalConfig      getRetrieval()   { return retrieval; }
        public boolean isRouter()                    { return "router".equals(type); }
        public boolean isLeaf()                      { return "leaf".equals(type); }
    }

    // ------------------------------------------------------------------
    // Location config (used by DateTimeTools)
    // ------------------------------------------------------------------

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class LocationConfig
    {
        /** City name passed to Hebcal and zmanim APIs, e.g. "Jerusalem" or "New York". */
        @JsonProperty("city")
        private String city = "Jerusalem";

        /**
         * IANA timezone ID for the default location.
         * Used by DateTimeTools.getCurrentDateTime() to display local time.
         * Examples: "Asia/Jerusalem", "America/New_York", "Europe/London".
         */
        @JsonProperty("timezone")
        private String timezone = "Asia/Jerusalem";

        public String getCity()     { return city; }
        public String getTimezone() { return timezone; }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class AgentConfig
    {
        // ------------------------------------------------------------------
        // Provider credentials
        // Per-provider configs; each key is a provider name (anthropic, openai, etc.)
        // API keys fall back to environment variables if blank/absent.
        // ------------------------------------------------------------------

        @JsonProperty("providers")
        private Map<String, ProviderConfig> providers = new HashMap<>();

        // ------------------------------------------------------------------
        // Default models
        // ------------------------------------------------------------------

        /**
         * Default model used for leaf agents when no per-node override is set.
         * Provider-specific API key is resolved from providers map or env var.
         */
        @JsonProperty("defaultModel")
        private ModelConfig defaultModel = new ModelConfig();  // anthropic / claude-sonnet-4-6

        /**
         * Lightweight model used for routing decisions.
         * Defaults to defaultModel if not set.
         * Recommended: a fast, cheap model (e.g. claude-haiku-3-5 or gpt-4o-mini).
         */
        @JsonProperty("routerModel")
        private ModelConfig routerModel;

        /**
         * Embedding model used by SefariaRetriever to embed queries.
         * Almost always OpenAI text-embedding-3-small to match the ingested vectors.
         */
        @JsonProperty("embeddingModel")
        private String embeddingModel = "text-embedding-3-small";

        // ------------------------------------------------------------------
        // Qdrant
        // ------------------------------------------------------------------

        @JsonProperty("qdrantHost")
        private String qdrantHost = "localhost";

        @JsonProperty("qdrantPort")
        private int qdrantPort = 6334;

        @JsonProperty("collectionName")
        private String collectionName = "sefaria_texts";

        // ------------------------------------------------------------------
        // Conversation memory (per leaf agent)
        // ------------------------------------------------------------------

        @JsonProperty("chatMemorySize")
        private int chatMemorySize = 20;

        // ------------------------------------------------------------------
        // Global tools and MCP servers
        //
        // Tools listed here are given to EVERY leaf agent in the tree,
        // regardless of that leaf's own "tools" array.  Use this for
        // cross-cutting tools that any agent may need (e.g. DATE_TIME).
        //
        // Per-leaf tools are additive: a leaf receives global tools PLUS
        // whatever is listed in its own "tools" array.
        // ------------------------------------------------------------------

        /**
         * Tool names (matching ToolRegistry constants) given to every leaf.
         * Example: ["DATE_TIME"]
         */
        @JsonProperty("globalTools")
        private List<String> globalTools = List.of();

        /**
         * MCP server configs connected to every leaf agent.
         * Useful for general-purpose servers (date/time, utility).
         */
        @JsonProperty("globalMcpServers")
        private List<McpServerConfig> globalMcpServers = List.of();

        // ------------------------------------------------------------------
        // Default location (for zmanim / date-time tools)
        // ------------------------------------------------------------------

        /**
         * Default city and timezone used by DateTimeTools.getDefaultLocation()
         * and as a fallback for zmanim tools when the user does not specify a city.
         */
        @JsonProperty("defaultLocation")
        private LocationConfig defaultLocation = new LocationConfig();

        // ------------------------------------------------------------------
        // Agent tree
        // ------------------------------------------------------------------

        /**
         * Root of the agent tree.  Typically a router whose children are
         * domain-specific routers or leaf agents.
         */
        @JsonProperty("tree")
        private AgentNodeConfig tree;

        // ------------------------------------------------------------------
        // HTTP server
        // ------------------------------------------------------------------

        /** Port the Vert.x HTTP server listens on. Default: 8080. */
        @JsonProperty("httpPort")
        private int httpPort = 8080;

        /**
         * When true, the agent captures every tool call and its result and
         * returns them in the HTTP response's {@code debugEvents} array.
         * Has no effect in CLI mode (use log level DEBUG there instead).
         */
        @JsonProperty("debugMode")
        private boolean debugMode = false;

        // ------------------------------------------------------------------
        // Accessors
        // ------------------------------------------------------------------

        public Map<String, ProviderConfig> getProviders()          { return providers; }
        public ModelConfig  getDefaultModel()                      { return defaultModel; }
        public ModelConfig  getRouterModel()                       { return routerModel != null ? routerModel : defaultModel; }
        public String       getEmbeddingModel()                    { return embeddingModel; }
        public String       getQdrantHost()                        { return qdrantHost; }
        public int          getQdrantPort()                        { return qdrantPort; }
        public String       getCollectionName()                    { return collectionName; }
        public int          getChatMemorySize()                    { return chatMemorySize; }
        public List<String> getGlobalTools()                       { return globalTools; }
        public List<McpServerConfig> getGlobalMcpServers()        { return globalMcpServers; }
        public LocationConfig getDefaultLocation()                 { return defaultLocation; }
        public AgentNodeConfig getTree()                           { return tree; }
        public int          getHttpPort()                          { return httpPort; }
        public boolean      isDebugMode()                          { return debugMode; }

        /**
         * Resolve the API key for a provider: config file first, env var fallback.
         */
        public String resolveApiKey(String provider)
        {
            ProviderConfig pc = providers.get(provider);
            String key = (pc != null) ? pc.getApiKey() : null;
            if (key != null && !key.isBlank())
            {
                return key;
            }
            // Env var fallbacks
            return switch (provider.toLowerCase())
            {
                case "anthropic" -> System.getenv("ANTHROPIC_API_KEY");
                case "openai"    -> System.getenv("OPENAI_API_KEY");
                case "google"    -> System.getenv("GOOGLE_API_KEY");
                case "mistral"   -> System.getenv("MISTRAL_API_KEY");
                default          -> null;
            };
        }

        public String resolveBaseUrl(String provider)
        {
            ProviderConfig pc = providers.get(provider);
            return pc != null ? pc.getBaseUrl() : null;
        }
    }
}
