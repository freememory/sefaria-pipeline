package org.freememory.pipeline.agent;

import dev.langchain4j.data.message.SystemMessage;
import dev.langchain4j.data.message.UserMessage;
import dev.langchain4j.model.chat.ChatModel;
import dev.langchain4j.model.chat.response.ChatResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * An internal node in the agent tree that classifies messages and delegates to
 * the most appropriate child node.
 *
 * === Routing mechanism ===
 *
 * On each call, the router presents the LLM with:
 *   1. A system message listing all children by name + description
 *   2. The user's original message
 *
 * The LLM is instructed to respond with ONLY the exact name of the chosen
 * child.  If the response doesn't match any child name (hallucination or
 * formatting noise) the router falls back to the first child and logs a
 * warning.
 *
 * === Model choice ===
 *
 * Routers use the lightweight "router model" from config (e.g. claude-haiku-4-5)
 * — the classification task is simple and does not need a large model.
 * Leaves use the full-size "default model" (e.g. claude-sonnet-4-6).
 * Each node can also override its model individually in the config JSON.
 *
 * === No memory needed ===
 *
 * Routing decisions are stateless — the router does not need conversation
 * history to classify a single message.  Only leaf nodes maintain chat memory.
 * The routing decision is recorded in {@link ConversationContext#recordRouting}
 * for debugging.
 */
public class RouterNode implements AgentNode
{
    private static final Logger log = LoggerFactory.getLogger(RouterNode.class);

    private final String          name;
    private final String          description;
    private final ChatModel       routerModel;
    private final List<AgentNode> children;

    public RouterNode(String name,
                      String description,
                      ChatModel routerModel,
                      List<AgentNode> children)
    {
        if (children == null || children.isEmpty())
        {
            throw new IllegalArgumentException(
                    "RouterNode '" + name + "' must have at least one child.");
        }
        this.name        = name;
        this.description = description;
        this.routerModel = routerModel;
        this.children    = List.copyOf(children);
    }

    // ------------------------------------------------------------------
    // AgentNode
    // ------------------------------------------------------------------

    @Override
    public String getName()
    {
        return name;
    }

    @Override
    public String getDescription()
    {
        return description;
    }

    @Override
    public String handle(ConversationContext ctx, String userMessage)
    {
        // Always call the routing LLM, but pass in the currently-pinned agent
        // (if any) so it can distinguish continuations from genuine topic pivots.
        String currentAgent = ctx.rememberedChild(name);
        AgentNode chosen = route(userMessage, currentAgent);
        ctx.rememberChild(name, chosen.getName());
        ctx.recordRouting(name, chosen.getName());

        String suffix = currentAgent == null                       ? ""
                      : chosen.getName().equals(currentAgent)      ? " (sticky)"
                      :                                              " (pivot from " + currentAgent + ")";
        log.info("[{}] → [{}]{}", name, chosen.getName(), suffix);

        return chosen.handle(ctx, userMessage);
    }

    // ------------------------------------------------------------------
    // Private
    // ------------------------------------------------------------------

    private AgentNode route(String userMessage, String currentAgent)
    {
        String systemText = buildRoutingPrompt(currentAgent);
        ChatResponse response;

        try
        {
            response = routerModel.chat(
                    SystemMessage.from(systemText),
                    UserMessage.from(userMessage)
            );
        }
        catch (Exception e)
        {
            // On LLM failure: stay with the current agent if there is one,
            // otherwise fall back to the first child.
            AgentNode fallback = (currentAgent != null)
                    ? children.stream()
                              .filter(c -> c.getName().equals(currentAgent))
                              .findFirst()
                              .orElse(children.get(0))
                    : children.get(0);
            log.warn("[{}] Routing LLM call failed ({}), falling back to '{}'",
                    name, e.getMessage(), fallback.getName());
            return fallback;
        }

        String choice = response.aiMessage().text().strip();
        log.debug("[{}] Routing response: \"{}\"", name, choice);

        // Find a child whose name matches the LLM's response (case-insensitive)
        for (AgentNode child : children)
        {
            if (child.getName().equalsIgnoreCase(choice))
            {
                return child;
            }
        }

        // Fuzzy fallback: partial match (handles the LLM adding punctuation)
        for (AgentNode child : children)
        {
            if (choice.toLowerCase().contains(child.getName().toLowerCase())
                    || child.getName().toLowerCase().contains(choice.toLowerCase()))
            {
                log.debug("[{}] Fuzzy-matched \"{}\" → '{}'", name, choice, child.getName());
                return child;
            }
        }

        log.warn("[{}] Could not match routing response \"{}\" to any child. "
                + "Available: {}. Falling back to '{}'.",
                name, choice, childNames(), children.get(0).getName());
        return children.get(0);
    }

    private String buildRoutingPrompt(String currentAgent)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("You are a routing assistant for a Jewish learning AI system.\n");

        if (currentAgent != null)
        {
            sb.append("The conversation so far has been handled by the [")
              .append(currentAgent)
              .append("] agent.\n");
            sb.append("If the new message is a continuation, follow-up, or short reply ")
              .append("(e.g. \"yes\", \"continue\", \"tell me more\", or a related question), ")
              .append("respond with: ").append(currentAgent).append("\n");
            sb.append("Only choose a different agent if the message is clearly ")
              .append("a new topic that another agent handles better.\n\n");
        }
        else
        {
            sb.append("Read the user's question and select the most appropriate agent.\n\n");
        }

        sb.append("Available agents:\n");
        for (AgentNode child : children)
        {
            sb.append("  - ").append(child.getName())
              .append(": ").append(child.getDescription()).append("\n");
        }
        sb.append("\nRespond with ONLY the exact name of the chosen agent ");
        sb.append("(copy it letter-for-letter from the list above). ");
        sb.append("Do not add any explanation or punctuation.");
        return sb.toString();
    }

    private String childNames()
    {
        return children.stream()
                .map(AgentNode::getName)
                .reduce((a, b) -> a + ", " + b)
                .orElse("(none)");
    }
}
