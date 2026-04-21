package org.freememory.pipeline.agent;

import dev.langchain4j.memory.ChatMemory;
import dev.langchain4j.memory.chat.MessageWindowChatMemory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Session state for one conversation — shared across the entire agent tree.
 *
 * === Per-leaf chat memory ===
 *
 * Each leaf agent gets its own {@link ChatMemory} (a sliding window of the last
 * N user+assistant turns). Memories are lazily created on first access.
 *
 * This means that if routing sends three consecutive questions to the Shabbat
 * agent, then switches to Kashrut, and then returns to Shabbat, the Shabbat
 * agent still remembers the three earlier questions.  The Kashrut agent starts
 * fresh for its first question in this session.
 *
 * For a future multi-user web layer, swap this single-instance design for a
 * session-ID-keyed approach using LangChain4j's {@code ChatMemoryProvider}.
 *
 * === Routing path log ===
 *
 * Each routing decision (router name → child name) is appended to the path log
 * so you can inspect how a question was delegated at any point.
 */
public class ConversationContext
{
    private final int    memorySize;
    private final String sessionId;

    private final Map<String, ChatMemory> leafMemories  = new LinkedHashMap<>();
    private final List<String>            routingPath   = new ArrayList<>();

    /**
     * Sticky routing: maps each router's name to the child it last selected.
     *
     * Persists across turns so follow-up messages ("continue", "tell me more")
     * go to the same leaf without being mis-routed.  Cleared only when the user
     * starts a new conversation (/new), which creates a fresh ConversationContext.
     */
    private final Map<String, String> lastChildByRouter = new LinkedHashMap<>();

    /** CLI mode — single session, fixed ID. */
    public ConversationContext(int memorySize)
    {
        this(memorySize, "default");
    }

    /** HTTP mode — each browser session gets a unique UUID as its ID. */
    public ConversationContext(int memorySize, String sessionId)
    {
        this.memorySize = memorySize;
        this.sessionId  = sessionId;
    }

    /** Session identifier passed to {@link GenericLeafAgent} as {@code @MemoryId}. */
    public String getSessionId()
    {
        return sessionId;
    }

    // ------------------------------------------------------------------
    // Chat memory
    // ------------------------------------------------------------------

    /**
     * Return the chat memory for the named leaf, creating it on first access.
     * The memory is a sliding window of at most {@code memorySize} messages.
     */
    public ChatMemory getMemoryFor(String leafName)
    {
        return leafMemories.computeIfAbsent(
                leafName,
                k -> MessageWindowChatMemory.withMaxMessages(memorySize)
        );
    }

    // ------------------------------------------------------------------
    // Routing path
    // ------------------------------------------------------------------

    /**
     * Mark the start of a new user turn.
     *
     * Call this before each {@code root.handle()} invocation so that
     * {@link #getLastRoutingPath()} reflects only the current turn's routing
     * decisions rather than accumulating across turns.
     */
    public void startTurn()
    {
        routingPath.clear();
    }

    /** Record one routing decision: the router chose {@code childName}. */
    public void recordRouting(String routerName, String childName)
    {
        routingPath.add(routerName + " → " + childName);
    }

    /**
     * Return the full routing path for the current turn as a human-readable
     * string, e.g. "Sefaria Agent → Halakha → Shabbat".
     *
     * Each entry in the log is "RouterName → ChildName".  This method chains
     * them by taking the left side of the first entry and the right side of
     * each subsequent entry:
     *
     *   ["Sefaria Agent → Halakha", "Halakha → Shabbat"]
     *   → "Sefaria Agent → Halakha → Shabbat"
     */
    public String getLastRoutingPath()
    {
        if (routingPath.isEmpty())
        {
            return "(no routing yet)";
        }

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < routingPath.size(); i++)
        {
            String entry = routingPath.get(i);
            if (i == 0)
            {
                sb.append(entry);                          // "A → B"
            }
            else
            {
                int arrow = entry.indexOf(" → ");
                sb.append(entry.substring(arrow));         // " → C" (skip "B")
            }
        }
        return sb.toString();
    }

    // ------------------------------------------------------------------
    // Sticky routing
    // ------------------------------------------------------------------

    /**
     * Remember the child this router selected, so the next turn can skip
     * the routing LLM and go straight to the same agent.
     */
    public void rememberChild(String routerName, String childName)
    {
        lastChildByRouter.put(routerName, childName);
    }

    /**
     * Return the child this router last selected, or {@code null} if this
     * router has not been used in the current conversation yet.
     */
    public String rememberedChild(String routerName)
    {
        return lastChildByRouter.get(routerName);
    }
}
