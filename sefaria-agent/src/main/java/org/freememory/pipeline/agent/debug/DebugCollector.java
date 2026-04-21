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

    private DebugCollector() {}

    /** Begin a new collection window (clears any leftover events). */
    public static void start()
    {
        EVENTS.get().clear();
    }

    /** Append one debug line (called by the listener). */
    public static void record(String event)
    {
        EVENTS.get().add(event);
    }

    /** Return a snapshot of all events collected since the last {@link #start()}. */
    public static List<String> collect()
    {
        return new ArrayList<>(EVENTS.get());
    }

    /** Remove the ThreadLocal to avoid leaking memory in thread-pool environments. */
    public static void clear()
    {
        EVENTS.remove();
    }
}
