package org.freememory.pipeline.agent;

import dev.langchain4j.model.anthropic.AnthropicChatModel;
import dev.langchain4j.model.chat.ChatModel;
import dev.langchain4j.model.googleai.GoogleAiGeminiChatModel;
import dev.langchain4j.model.chat.listener.ChatModelListener;
import dev.langchain4j.model.mistralai.MistralAiChatModel;
import dev.langchain4j.model.ollama.OllamaChatModel;
import dev.langchain4j.model.openai.OpenAiChatModel;
import org.freememory.config.PipelineConfig.AgentConfig;
import org.freememory.config.PipelineConfig.ModelConfig;
import org.freememory.pipeline.agent.debug.DebugChatModelListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;

/**
 * Creates {@link ChatModel} instances from config, abstracting provider details.
 *
 * === Adding a new provider ===
 *
 * 1. Add the {@code langchain4j-<provider>} Maven dependency to pom.xml
 *    (use version property {@code ${langchain4j.version}}).
 * 2. Add a case to the switch in {@link #create}.
 * 3. List the provider name in your config JSON's {@code providers} map with
 *    an {@code apiKey} entry.
 *
 * === API key resolution order ===
 *
 * 1. {@code providers.<name>.apiKey} in the config file
 * 2. Standard environment variable (ANTHROPIC_API_KEY, OPENAI_API_KEY, …)
 *
 * The config-file key is never committed to a public repository; use it only
 * for local development.
 */
public final class ModelFactory
{
    private static final Logger log = LoggerFactory.getLogger(ModelFactory.class);

    /**
     * Per-request HTTP timeout for all LLM providers.
     *
     * 60 s (the LangChain4j default) is too short for complex agent chains:
     * a Halakha query may invoke 3-4 tools, each needing its own round-trip,
     * before the final LLM completion.  3 minutes gives ample headroom while
     * still surfacing genuine hangs within a reasonable time.
     */
    private static final Duration REQUEST_TIMEOUT = Duration.ofMinutes(3);

    private ModelFactory() {}

    /**
     * Maximum retry attempts after a {@link dev.langchain4j.exception.RateLimitException}.
     *
     * The built model is wrapped in {@link RetryingChatModel} so rate-limit
     * errors inside the agentic tool loop are transparently retried with
     * exponential backoff (15 s → 30 s → 60 s) rather than surfacing as errors.
     */
    private static final int RATE_LIMIT_RETRIES = 3;

    /**
     * Build a {@link ChatModel} for the given {@link ModelConfig}.
     *
     * The returned model is always wrapped in {@link RetryingChatModel} so
     * transient rate-limit errors are handled automatically.
     *
     * @param cfg       provider + modelId to instantiate
     * @param agentCfg  root agent config (used to resolve API keys and base URLs)
     * @throws IllegalArgumentException for unknown or misconfigured providers
     */
    public static ChatModel create(ModelConfig cfg, AgentConfig agentCfg)
    {
        String provider   = cfg.getProvider().toLowerCase();
        String modelId    = cfg.getModelId();
        int    maxTokens  = cfg.getMaxTokens();
        String apiKey     = agentCfg.resolveApiKey(provider);
        String baseUrl    = agentCfg.resolveBaseUrl(provider);

        // Always attach the debug listener — collection is unconditional and
        // very cheap (a few list.add() calls per request).  The HTTP API
        // always returns debugEvents; the UI checkbox controls whether they
        // are rendered, so no server restart is needed to toggle debug view.
        List<ChatModelListener> listeners = List.of(new DebugChatModelListener());

        log.debug("Creating ChatModel: provider={} modelId={} maxTokens={} debug={}",
                provider, modelId, maxTokens, agentCfg.isDebugMode());

        ChatModel raw = switch (provider)
        {
            case "anthropic" ->
            {
                requireKey(apiKey, "anthropic", "ANTHROPIC_API_KEY");
                yield AnthropicChatModel.builder()
                        .apiKey(apiKey)
                        .modelName(modelId)
                        .maxTokens(maxTokens)
                        .timeout(REQUEST_TIMEOUT)
                        .listeners(listeners)
                        .build();
            }
            case "openai" ->
            {
                requireKey(apiKey, "openai", "OPENAI_API_KEY");
                // Use maxCompletionTokens (sends max_completion_tokens) rather than
                // maxTokens (sends max_tokens).  The older max_tokens parameter is
                // deprecated and rejected outright by reasoning models (o1, o3, o4-mini).
                yield OpenAiChatModel.builder()
                        .apiKey(apiKey)
                        .modelName(modelId)
                        .maxCompletionTokens(maxTokens)
                        .timeout(REQUEST_TIMEOUT)
                        .listeners(listeners)
                        .build();
            }
            case "google" ->
            {
                requireKey(apiKey, "google", "GOOGLE_API_KEY");
                yield GoogleAiGeminiChatModel.builder()
                        .apiKey(apiKey)
                        .modelName(modelId)
                        .maxOutputTokens(maxTokens)
                        .timeout(REQUEST_TIMEOUT)
                        .listeners(listeners)
                        .build();
            }
            case "mistral" ->
            {
                requireKey(apiKey, "mistral", "MISTRAL_API_KEY");
                yield MistralAiChatModel.builder()
                        .apiKey(apiKey)
                        .modelName(modelId)
                        .maxTokens(maxTokens)
                        .timeout(REQUEST_TIMEOUT)
                        .listeners(listeners)
                        .build();
            }
            case "ollama" ->
            {
                String url = (baseUrl != null && !baseUrl.isBlank())
                        ? baseUrl : "http://localhost:11434";
                yield OllamaChatModel.builder()
                        .baseUrl(url)
                        .modelName(modelId)
                        .numPredict(maxTokens)
                        .listeners(listeners)
                        .build();
            }
            default -> throw new IllegalArgumentException(
                    "Unknown model provider: \"" + provider + "\". "
                    + "Supported: anthropic, openai, google, mistral, ollama"
            );
        };

        // Wrap with retry so transient rate-limit errors are handled
        // automatically inside the agentic tool loop.
        return new RetryingChatModel(raw, RATE_LIMIT_RETRIES);
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    private static void requireKey(String key, String provider, String envVar)
    {
        if (key == null || key.isBlank())
        {
            throw new IllegalStateException(
                    "No API key for provider \"" + provider + "\". "
                    + "Set providers." + provider + ".apiKey in your config file, "
                    + "or set the " + envVar + " environment variable."
            );
        }
    }
}
