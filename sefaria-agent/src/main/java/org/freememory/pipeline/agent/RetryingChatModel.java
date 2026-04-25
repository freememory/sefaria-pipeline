package org.freememory.pipeline.agent;

import dev.langchain4j.exception.RateLimitException;
import dev.langchain4j.model.chat.ChatModel;
import dev.langchain4j.model.chat.request.ChatRequest;
import dev.langchain4j.model.chat.response.ChatResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wraps a {@link ChatModel} to retry on {@link RateLimitException} with
 * exponential backoff.
 *
 * <p>Anthropic enforces a per-minute <em>input</em> token limit (30K on Tier 1).
 * When the agentic loop makes multiple tool calls in quick succession the limit
 * can be exhausted mid-chain.  Without retry the entire user request fails with
 * a visible error; with retry it pauses and recovers transparently.
 *
 * <h3>Backoff schedule (default 3 retries)</h3>
 * <pre>
 *   Attempt 1 failed → wait 15 s
 *   Attempt 2 failed → wait 30 s
 *   Attempt 3 failed → wait 60 s
 *   Attempt 4 failed → rethrow
 * </pre>
 *
 * <p>Each wait is long enough to allow Anthropic's 1-minute sliding window
 * to partially reset.  The exact position in the window is unknown, so 15 s
 * is a reasonable minimum rather than always waiting the full 60 s.
 */
public class RetryingChatModel implements ChatModel
{
    private static final Logger log = LoggerFactory.getLogger(RetryingChatModel.class);

    /** Base wait in milliseconds; doubles on each retry. */
    private static final long BASE_WAIT_MS = 15_000L;

    private final ChatModel delegate;
    private final int       maxRetries;

    /**
     * @param delegate   the underlying model to delegate all calls to
     * @param maxRetries number of retry attempts after the first failure
     *                   (total attempts = maxRetries + 1)
     */
    public RetryingChatModel(ChatModel delegate, int maxRetries)
    {
        this.delegate   = delegate;
        this.maxRetries = maxRetries;
    }

    @Override
    public ChatResponse chat(ChatRequest request)
    {
        for (int attempt = 0; attempt <= maxRetries; attempt++)
        {
            try
            {
                return delegate.chat(request);
            }
            catch (RateLimitException e)
            {
                if (attempt == maxRetries)
                {
                    log.error("Rate limit exceeded after {} attempts — giving up.", maxRetries + 1);
                    throw e;
                }

                long waitMs = BASE_WAIT_MS * (1L << attempt); // 15s, 30s, 60s
                log.warn("Rate limit hit (attempt {}/{}). Waiting {} s before retry …",
                         attempt + 1, maxRetries + 1, waitMs / 1000);
                try
                {
                    Thread.sleep(waitMs);
                }
                catch (InterruptedException ie)
                {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted during rate-limit backoff", ie);
                }
            }
        }

        // Unreachable — the loop always either returns or throws.
        throw new IllegalStateException("RetryingChatModel loop exited without result");
    }
}
