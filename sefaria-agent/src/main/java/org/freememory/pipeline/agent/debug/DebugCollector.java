package org.freememory.pipeline.agent.debug;

import java.util.ArrayList;
import java.util.List;

/**
 * Thread-local accumulator for tool-call debug events.
 *
 * Usage pattern in the HTTP request handler (all on the same worker thread):
 *
 *   DebugCollector.start();
 *   String answer = root.handle(ctx, message);   // fires listener callbacks
 *   List<String> events = DebugCollector.collect();
 *   DebugCollector.clear();
 *
 * The {@link DebugChatModelListener} calls {@link #record} from the same
 * thread, so no synchronisation is needed.
 */
public final class DebugCollector
{
    private static final ThreadLocal<List<String>> EVENTS =
            ThreadLocal.withInitial(ArrayList::new);

    /**
     * Set to {@code true} when the LLM's last response was cut off because it
     * hit the max-tokens limit ({@code finishReason=LENGTH}).
     */
    private static final ThreadLocal<Boolean> TRUNCATED =
            ThreadLocal.withInitial(() -> false);

    private DebugCollector() {}

    /** Begin a new collection window (clears any leftover state). */
    public static void start()
    {
        EVENTS.get().clear();
        TRUNCATED.set(false);
    }

    /** Append one debug line (called by the listener). */
    public static void record(String event)
    {
        EVENTS.get().add(event);
    }

    /**
     * Signal that the most recent LLM response was cut off at the token limit.
     * Called by {@link DebugChatModelListener} when it sees {@code finishReason=LENGTH}.
     */
    public static void setTruncated()
    {
        TRUNCATED.set(true);
    }

    /** Returns {@code true} if the response was cut off at the token limit. */
    public static boolean isTruncated()
    {
        return Boolean.TRUE.equals(TRUNCATED.get());
    }

    /** Return a snapshot of all events collected since the last {@link #start()}. */
    public static List<String> collect()
    {
        return new ArrayList<>(EVENTS.get());
    }

    /** Remove the ThreadLocals to avoid leaking memory in thread-pool environments. */
    public static void clear()
    {
        EVENTS.remove();
        TRUNCATED.remove();
    }
}
