package org.freememory.pipeline.agent;

/**
 * One node in the agent tree — either a router or a leaf.
 *
 * Routers classify the incoming message and delegate to the most appropriate
 * child node. Leaves perform the actual retrieval + LLM call and return a
 * response to the user.
 *
 * The tree is assembled at startup by {@link AgentTree} from the JSON config
 * and is then immutable for the lifetime of the process. Adding or removing an
 * agent is a config-file change with no code modifications required.
 */
public interface AgentNode
{
    /** Display name used in routing prompts and log messages. */
    String getName();

    /**
     * One-line description of this node's speciality.
     * Router nodes expose this to their parent router so it can decide whether
     * to delegate to this node.  Keep it concise and discriminating.
     */
    String getDescription();

    /**
     * Process one user message within the given conversation session.
     *
     * Implementations must:
     *   - Read prior history from {@code ctx} if needed for context
     *   - Add the user turn and the response to {@code ctx} (leaf nodes only —
     *     routers do not add their internal classification calls to the history)
     *   - Return the text response to show the user
     *
     * @param ctx         shared session state (per-leaf chat memories, routing path)
     * @param userMessage the user's raw input for this turn
     * @return the agent's response text
     */
    String handle(ConversationContext ctx, String userMessage);
}
