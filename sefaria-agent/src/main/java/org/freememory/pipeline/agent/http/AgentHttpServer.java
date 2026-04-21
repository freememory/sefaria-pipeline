package org.freememory.pipeline.agent.http;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import org.freememory.pipeline.agent.AgentNode;
import org.freememory.pipeline.agent.ConversationContext;
import org.freememory.pipeline.agent.debug.DebugCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Vert.x HTTP server exposing the agent tree as a JSON API and serving the
 * single-page web UI from a classpath resource.
 *
 * === API ===
 *
 *   GET  /              — serves web/index.html (the chat UI)
 *   POST /api/chat      — send a message, get a response
 *   POST /api/new       — discard the current session and start fresh
 *
 * === POST /api/chat request ===
 *
 *   { "message": "...", "sessionId": "uuid-or-omit-for-new" }
 *
 * === POST /api/chat response ===
 *
 *   { "response":     "...",
 *     "routingPath":  "Sefaria Agent → Halakha → Shabbat",
 *     "sessionId":    "uuid",
 *     "debugEvents":  ["⚙ getCurrentDateTime({})", "↩ getCurrentDateTime → ..."]
 *   }
 *
 * === Sessions ===
 *
 * Each browser tab/window carries its own UUID sessionId (stored in
 * localStorage).  The server maps sessionId → ConversationContext, so every
 * session has independent routing state and per-leaf chat memory.
 *
 * Note: sessions are never evicted in this single-user implementation.  For a
 * multi-user deployment add a TTL cache (e.g. Caffeine) keyed on sessionId.
 */
public class AgentHttpServer
{
    private static final Logger log = LoggerFactory.getLogger(AgentHttpServer.class);

    /** Seconds the browser should wait before auto-retrying on a rate-limit response. */
    private static final int RATE_LIMIT_RETRY_AFTER_SECS = 30;

    private final Vertx  vertx;
    private final AgentNode root;
    private final int    memorySize;
    private final String  indexHtml;

    private final Map<String, ConversationContext> sessions = new ConcurrentHashMap<>();

    public AgentHttpServer(AgentNode root, int memorySize, boolean debugMode)
    {
        this.root       = root;
        this.memorySize = memorySize;
        this.vertx      = Vertx.vertx();
        this.indexHtml  = loadIndexHtml();
    }

    // ------------------------------------------------------------------
    // Startup
    // ------------------------------------------------------------------

    public void start(int port)
    {
        Router router = Router.router(vertx);

        // Parse request bodies
        router.route().handler(BodyHandler.create());

        // Static UI
        router.get("/").handler(this::serveIndex);

        // API
        router.post("/api/chat").handler(this::handleChat);
        router.post("/api/new").handler(this::handleNew);

        vertx.createHttpServer()
             .requestHandler(router)
             .listen(port, result ->
             {
                 if (result.succeeded())
                 {
                     log.info("╔═══════════════════════════════════════════╗");
                     log.info("║  Sefaria Agent  →  http://localhost:{}  ║", port);
                     log.info("╚═══════════════════════════════════════════╝");
                 }
                 else
                 {
                     log.error("Failed to start HTTP server on port {}", port,
                             result.cause());
                 }
             });
    }

    // ------------------------------------------------------------------
    // Handlers
    // ------------------------------------------------------------------

    private void serveIndex(RoutingContext ctx)
    {
        ctx.response()
           .putHeader("Content-Type", "text/html; charset=UTF-8")
           .end(indexHtml);
    }

    private void handleChat(RoutingContext ctx)
    {
        JsonObject body;
        try
        {
            body = ctx.getBodyAsJson();
        }
        catch (Exception e)
        {
            badRequest(ctx, "Invalid JSON body");
            return;
        }

        String message   = body.getString("message", "").strip();
        String sessionId = body.getString("sessionId", "");

        if (message.isEmpty())
        {
            badRequest(ctx, "message must not be empty");
            return;
        }
        if (sessionId == null || sessionId.isBlank())
        {
            sessionId = UUID.randomUUID().toString();
        }

        // Capture the sessionId for the lambda (must be effectively final)
        final String sid = sessionId;
        final ConversationContext convCtx = sessions.computeIfAbsent(
                sid, id -> new ConversationContext(memorySize, id));

        // Run the blocking agent call on a worker thread so the event loop
        // stays free for other requests / keep-alives.
        vertx.executeBlocking(
                promise ->
                {
                    // Debug collection is always enabled; the UI checkbox controls
                    // rendering — no server restart needed to toggle debug view.
                    DebugCollector.start();
                    try
                    {
                        convCtx.startTurn();
                        String answer = handleWithRetry(convCtx, message, sid);
                        List<String> events = DebugCollector.collect();
                        promise.complete(new ChatResult(answer,
                                convCtx.getLastRoutingPath(), events, sid));
                    }
                    catch (Exception e)
                    {
                        promise.fail(e);
                    }
                    finally
                    {
                        DebugCollector.clear();
                    }
                },
                false,          // unordered — parallel requests are fine
                result ->
                {
                    if (result.succeeded())
                    {
                        ChatResult r = (ChatResult) result.result();
                        JsonObject json = new JsonObject()
                                .put("response",    r.response())
                                .put("routingPath", r.routingPath())
                                .put("sessionId",   r.sessionId())
                                .put("debugEvents", new JsonArray(r.debugEvents()));
                        respondJson(ctx, 200, json);
                    }
                    else
                    {
                        Throwable cause = result.cause();
                        if (isRateLimit(cause))
                        {
                            log.warn("Rate limit for session {}: returning 429", sid);
                            respondJson(ctx, 429, new JsonObject()
                                    .put("rateLimited", true)
                                    .put("retryAfter", RATE_LIMIT_RETRY_AFTER_SECS));
                        }
                        else
                        {
                            log.error("Agent error for session {}", sid, cause);
                            respondJson(ctx, 500, new JsonObject()
                                    .put("error", cause.getMessage()));
                        }
                    }
                }
        );
    }

    /**
     * Calls the agent tree, letting all exceptions propagate immediately.
     * Rate-limit errors surface as HTTP 429 with a retryAfter hint so the
     * browser can count down and re-send — no worker thread is blocked waiting.
     */
    private String handleWithRetry(ConversationContext convCtx,
                                   String message,
                                   String sid) throws Exception
    {
        return root.handle(convCtx, message);
    }

    /** Returns true if the exception is a provider rate-limit error. */
    private static boolean isRateLimit(Throwable t)
    {
        if (t == null)
        {
            return false;
        }
        String msg = t.getMessage();
        return msg != null && (msg.contains("rate_limit_error")
                || msg.contains("rate limit")
                || msg.contains("429"));
    }

    private void handleNew(RoutingContext ctx)
    {
        JsonObject body;
        try
        {
            body = ctx.getBodyAsJson();
        }
        catch (Exception e)
        {
            body = new JsonObject();
        }

        String oldSession = body.getString("sessionId", "");
        if (oldSession != null && !oldSession.isBlank())
        {
            sessions.remove(oldSession);
        }

        String newSession = UUID.randomUUID().toString();
        sessions.put(newSession, new ConversationContext(memorySize, newSession));

        respondJson(ctx, 200, new JsonObject()
                .put("sessionId", newSession)
                .put("message",   "New conversation started"));
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    private static void respondJson(RoutingContext ctx, int status, JsonObject body)
    {
        ctx.response()
           .setStatusCode(status)
           .putHeader("Content-Type", "application/json; charset=UTF-8")
           .end(body.encode());
    }

    private static void badRequest(RoutingContext ctx, String reason)
    {
        respondJson(ctx, 400, new JsonObject().put("error", reason));
    }

    private static String loadIndexHtml()
    {
        try (InputStream is = AgentHttpServer.class
                .getClassLoader()
                .getResourceAsStream("web/index.html"))
        {
            if (is == null)
            {
                log.error("web/index.html not found on classpath");
                return "<h1>Error: web/index.html not found</h1>";
            }
            return new String(is.readAllBytes(), StandardCharsets.UTF_8);
        }
        catch (Exception e)
        {
            log.error("Failed to load web/index.html", e);
            return "<h1>Error loading UI: " + e.getMessage() + "</h1>";
        }
    }

    // ------------------------------------------------------------------
    // Internal record
    // ------------------------------------------------------------------

    private record ChatResult(String response,
                               String routingPath,
                               List<String> debugEvents,
                               String sessionId) {}
}
