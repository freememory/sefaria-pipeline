package org.freememory.scripts;

import dev.langchain4j.model.embedding.EmbeddingModel;
import dev.langchain4j.model.openai.OpenAiEmbeddingModel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.qdrant.client.QdrantClient;
import io.qdrant.client.QdrantGrpcClient;
import io.qdrant.client.grpc.Collections.CollectionInfo;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.freememory.config.ConfigLoader;
import org.freememory.config.PipelineConfig;
import org.freememory.config.PipelineConfig.AgentConfig;
import org.freememory.pipeline.agent.AgentNode;
import org.freememory.pipeline.agent.AgentTree;
import org.freememory.pipeline.agent.ConversationContext;
import org.freememory.pipeline.agent.http.AgentHttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Scanner;

/**
 * Phase 4 entrypoint: runs the Sefaria agent either as an HTTP server (default)
 * or as an interactive CLI REPL (pass {@code --mode cli}).
 *
 * === HTTP mode (default) ===
 *
 *   Starts a Vert.x HTTP server on the configured port (default 8080).
 *   Open http://localhost:8080 in a browser to use the chat UI.
 *
 *   Config options (pipeline-agent.json → agent section):
 *     httpPort   — TCP port to listen on (default: 8080)
 *     debugMode  — if true, include tool-call traces in API responses (default: false)
 *
 * === CLI mode ===
 *
 *   Pass {@code --mode cli} to use the original stdin/stdout REPL instead.
 *   Useful for quick debugging without a browser.
 *
 *   CLI commands:
 *     /exit    — quit
 *     /new     — start a new conversation (clears chat memory)
 *     /path    — show the routing path taken for the last message
 *     /help    — show available commands
 *
 * === Prerequisites ===
 *
 *   - Qdrant running locally with the sefaria_texts collection populated
 *       docker run -d -p 6333:6333 -p 6334:6334 \
 *           -v /path/to/qdrant_storage:/qdrant/storage qdrant/qdrant
 *
 *   - API keys set in config file or environment:
 *       ANTHROPIC_API_KEY   (for Claude — default agent model)
 *       OPENAI_API_KEY      (for embeddings — text-embedding-3-small)
 *
 * === Usage ===
 *
 *   java -jar sefaria-agent.jar
 *   java -jar sefaria-agent.jar --config config/pipeline-agent.json
 *   java -jar sefaria-agent.jar --mode cli
 *   java -jar sefaria-agent.jar --config config/pipeline-agent.json --mode cli
 */
public class RunAgentScript
{
    private static final Logger log = LoggerFactory.getLogger(RunAgentScript.class);

    public static void main(String[] args) throws Exception
    {
        boolean cliMode = Arrays.asList(args).contains("--mode") &&
                          Arrays.asList(args).indexOf("--mode") < args.length - 1 &&
                          "cli".equalsIgnoreCase(args[Arrays.asList(args).indexOf("--mode") + 1]);

        PipelineConfig config = ConfigLoader.load(args);
        AgentConfig ac = config.getAgent();

        log.info("=== Sefaria Agent ===");
        log.info("Default model:  {}/{}", ac.getDefaultModel().getProvider(),
                                          ac.getDefaultModel().getModelId());
        log.info("Router model:   {}/{}", ac.getRouterModel().getProvider(),
                                          ac.getRouterModel().getModelId());
        log.info("Qdrant:         {}:{}", ac.getQdrantHost(), ac.getQdrantPort());
        log.info("Collection:     {}", ac.getCollectionName());
        log.info("Mode:           {}", cliMode ? "CLI" : "HTTP (port " + ac.getHttpPort() + ")");
        log.info("Debug mode:     {}", ac.isDebugMode());

        // Resolve OpenAI key for the embedding model
        String openAiKey = ac.resolveApiKey("openai");
        if (openAiKey == null || openAiKey.isBlank())
        {
            log.error("No OpenAI API key found. The embedding model requires OpenAI.");
            log.error("Set providers.openai.apiKey in your config or OPENAI_API_KEY env var.");
            System.exit(1);
        }

        // Connect to Qdrant — use a custom ManagedChannel with keepalive so the
        // gRPC connection is not silently dropped after a period of inactivity.
        // Without this, Qdrant closes the TCP connection and subsequent calls
        // fail with UNAVAILABLE: "Network closed for unknown reason".
        ManagedChannel qdrantChannel = ManagedChannelBuilder
                .forAddress(ac.getQdrantHost(), ac.getQdrantPort())
                .usePlaintext()
                .keepAliveTime(30, TimeUnit.SECONDS)
                .keepAliveTimeout(10, TimeUnit.SECONDS)
                .keepAliveWithoutCalls(true)
                .build();

        QdrantClient qdrant = new QdrantClient(
                QdrantGrpcClient.newBuilder(qdrantChannel).build()
        );

        // Fail fast if Qdrant is unreachable or the collection is missing / empty.
        checkQdrantHealth(qdrant, ac.getQdrantHost(), ac.getQdrantPort(),
                          ac.getCollectionName());

        try
        {
            // Build the embedding model (must match the model used during ingest)
            EmbeddingModel embeddingModel = OpenAiEmbeddingModel.builder()
                    .apiKey(openAiKey)
                    .modelName(ac.getEmbeddingModel())
                    .build();

            // Build the agent tree from config
            AgentTree tree = new AgentTree(ac, qdrant, embeddingModel);
            AgentNode root = tree.build();

            if (cliMode)
            {
                System.out.println();
                System.out.println("╔══════════════════════════════════════════════╗");
                System.out.println("║      Sefaria Jewish Learning Agent (CLI)     ║");
                System.out.println("╚══════════════════════════════════════════════╝");
                System.out.println("  Type your question and press Enter.");
                System.out.println("  Commands: /exit  /new  /path  /help");
                System.out.println();

                runLoop(root, ac.getChatMemorySize());
            }
            else
            {
                // HTTP mode — start server and block the main thread so the JVM
                // keeps running while Vert.x handles requests on its event loop.
                AgentHttpServer server = new AgentHttpServer(
                        root, ac.getChatMemorySize(), ac.isDebugMode());
                server.start(ac.getHttpPort());

                // Park the main thread indefinitely; Vert.x runs on daemon threads
                // and the JVM would exit immediately without this.
                Thread.currentThread().join();
            }
        }
        finally
        {
            qdrant.close();
        }
    }

    // ------------------------------------------------------------------
    // Startup health check
    // ------------------------------------------------------------------

    /**
     * Verifies that Qdrant is reachable and the target collection exists and
     * contains vectors. Logs a clear, actionable error and calls
     * {@link System#exit(int)} if any check fails — better to stop immediately
     * than to discover the problem on the first user query.
     *
     * <p>Checks performed, in order:
     * <ol>
     *   <li>gRPC connectivity — a 5-second timeout catches connection-refused
     *       and DNS failures before the user can even type a question.</li>
     *   <li>Collection existence — the named collection must have been created
     *       by the ingest pipeline (Phase 3) before the agent can run.</li>
     *   <li>Non-zero vector count — warns (but does not exit) if the collection
     *       exists but is empty, which usually means ingest was not run yet.</li>
     * </ol>
     */
    private static void checkQdrantHealth(QdrantClient qdrant,
                                          String host, int port,
                                          String collectionName)
    {
        log.info("Checking Qdrant at {}:{} ...", host, port);

        // 1 — Connectivity: collectionExistsAsync makes a real gRPC call.
        //     A 5-second timeout is generous; a running Qdrant responds in <50 ms.
        boolean exists;
        try
        {
            exists = qdrant.collectionExistsAsync(collectionName)
                           .get(5, TimeUnit.SECONDS);
        }
        catch (TimeoutException e)
        {
            log.error("Qdrant health check timed out after 5 seconds.");
            log.error("Is Qdrant running at {}:{}?", host, port);
            log.error("Start it with:");
            log.error("  docker run -d -p 6333:6333 -p 6334:6334 qdrant/qdrant");
            System.exit(1);
            return; // unreachable — keeps compiler happy
        }
        catch (ExecutionException e)
        {
            log.error("Cannot connect to Qdrant at {}:{}: {}", host, port,
                      e.getCause() != null ? e.getCause().getMessage() : e.getMessage());
            log.error("Is Qdrant running? Start it with:");
            log.error("  docker run -d -p 6333:6333 -p 6334:6334 qdrant/qdrant");
            System.exit(1);
            return;
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
            log.error("Interrupted while checking Qdrant health.");
            System.exit(1);
            return;
        }

        // 2 — Collection existence
        if (!exists)
        {
            log.error("Collection '{}' does not exist in Qdrant.", collectionName);
            log.error("Run the ingest pipeline first (Phase 3) to populate the collection:");
            log.error("  java -jar sefaria-ingest.jar --config <your-config.json>");
            System.exit(1);
            return;
        }

        // 3 — Vector count (warn only — a near-empty collection is unusual but not fatal)
        try
        {
            CollectionInfo info = qdrant.getCollectionInfoAsync(collectionName)
                                        .get(5, TimeUnit.SECONDS);
            long vectorCount = info.getVectorsCount();
            if (vectorCount == 0)
            {
                log.warn("Collection '{}' exists but contains 0 vectors.", collectionName);
                log.warn("Queries will return no results. Run the ingest pipeline to populate it.");
            }
            else
            {
                log.info("Qdrant OK — collection '{}' has {} vectors.", collectionName, vectorCount);
            }
        }
        catch (Exception e)
        {
            // Collection info is non-critical — we know the collection exists, so continue.
            log.warn("Could not retrieve collection info (non-fatal): {}", e.getMessage());
        }
    }

    // ------------------------------------------------------------------
    // REPL
    // ------------------------------------------------------------------

    private static void runLoop(AgentNode root, int memorySize) throws Exception
    {
        Scanner scanner = new Scanner(System.in);
        // Use a single-element array so the reference can be replaced when
        // the user issues /new without needing a separate mutable wrapper class.
        ConversationContext[] ctxRef = { new ConversationContext(memorySize) };

        while (true)
        {
            System.out.print("You: ");
            System.out.flush();

            if (!scanner.hasNextLine())
            {
                break; // EOF (e.g. piped input)
            }

            String input = scanner.nextLine().strip();
            if (input.isBlank())
            {
                continue;
            }

            // CLI commands
            if (input.startsWith("/"))
            {
                if (handleCommand(input, ctxRef, memorySize))
                {
                    break; // /exit
                }
                continue;
            }

            // Regular message → route through the agent tree
            try
            {
                ConversationContext ctx = ctxRef[0];
                ctx.startTurn();
                String response = root.handle(ctx, input);
                System.out.println();
                System.out.println("Agent [" + ctx.getLastRoutingPath() + "]:");
                System.out.println(response);
                System.out.println();
            }
            catch (Exception e)
            {
                log.error("Error processing message", e);
                System.out.println("Error: " + e.getMessage());
                System.out.println();
            }
        }

        System.out.println("Goodbye.");
    }

    /**
     * Handle a CLI slash command.
     *
     * @param ctxRef single-element array holding the current ConversationContext;
     *               replaced in-place when the user issues /new
     * @return true if the loop should exit (/exit)
     */
    private static boolean handleCommand(String input,
                                         ConversationContext[] ctxRef,
                                         int memorySize)
    {
        String cmd = input.toLowerCase();

        switch (cmd)
        {
            case "/exit", "/quit" ->
            {
                return true;
            }
            case "/new" ->
            {
                ctxRef[0] = new ConversationContext(memorySize);
                System.out.println("[New conversation started — all chat memory cleared]");
                System.out.println();
            }
            case "/path" ->
            {
                System.out.println("Last routing path: " + ctxRef[0].getLastRoutingPath());
                System.out.println();
            }
            case "/help" ->
            {
                System.out.println("Commands:");
                System.out.println("  /exit   — quit the agent");
                System.out.println("  /new    — start a new conversation (clears all memory)");
                System.out.println("  /path   — show how the last message was routed");
                System.out.println("  /help   — show this help");
                System.out.println();
            }
            default ->
            {
                System.out.println("Unknown command: " + input
                        + "  (try /help for available commands)");
                System.out.println();
            }
        }

        return false;
    }
}
