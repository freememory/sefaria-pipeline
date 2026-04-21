package org.freememory.pipeline.agent.debug;

import dev.langchain4j.agent.tool.ToolExecutionRequest;
import dev.langchain4j.data.message.AiMessage;
import dev.langchain4j.data.message.ChatMessage;
import dev.langchain4j.data.message.SystemMessage;
import dev.langchain4j.data.message.ToolExecutionResultMessage;
import dev.langchain4j.data.message.UserMessage;
import dev.langchain4j.model.chat.listener.ChatModelErrorContext;
import dev.langchain4j.model.chat.listener.ChatModelListener;
import dev.langchain4j.model.chat.listener.ChatModelRequestContext;
import dev.langchain4j.model.chat.listener.ChatModelResponseContext;
import dev.langchain4j.model.output.FinishReason;

import java.util.List;

/**
 * Intercepts LangChain4j chat model calls and records debug events to
 * {@link DebugCollector} so the HTTP response can include a full trace.
 *
 * Three categories of event are recorded:
 *
 *   📋 Full augmented prompt  — system + augmented user message on the initial
 *                               call of each turn (before any tool calls).
 *   ⚙ toolName({args})       — tool call requested by the LLM.
 *   ↩ toolName → result      — tool result sent back to the LLM (truncated).
 */
public class DebugChatModelListener implements ChatModelListener
{

    /**
     * Called just before each LLM request.
     *
     * On the initial call of a turn (no tool-result messages present yet) the
     * full augmented prompt is recorded so the debug panel shows exactly what
     * was sent to the model — including the Sefaria context injected by the
     * retrieval augmentor.
     *
     * On subsequent calls (tool-loop iterations) only the tool results are
     * recorded, since the prompt itself hasn't changed.
     */
    @Override
    public void onRequest(ChatModelRequestContext ctx)
    {
        List<ChatMessage> messages = ctx.chatRequest().messages();

        boolean hasToolResults = messages.stream()
                .anyMatch(m -> m instanceof ToolExecutionResultMessage);

        if (!hasToolResults)
        {
            // Initial call — record the full augmented prompt.
            recordPrompt(messages);
        }
        else
        {
            // Tool-loop iteration — record the tool results being fed back.
            for (ChatMessage msg : messages)
            {
                if (msg instanceof ToolExecutionResultMessage result)
                {
                    DebugCollector.record("↩ " + result.toolName() + " → " + result.text());
                }
            }
        }
    }

    /** Called just after each LLM response — captures tool call requests and truncation. */
    @Override
    public void onResponse(ChatModelResponseContext ctx)
    {
        var response = ctx.chatResponse();
        var ai       = response.aiMessage();

        // Detect hard token-limit cutoff.  finishReason=LENGTH means the model
        // stopped because it ran out of output tokens, not because it naturally
        // finished — the response is incomplete.
        if (response.finishReason() == FinishReason.LENGTH)
        {
            DebugCollector.setTruncated();
            DebugCollector.record("⚠️ Response truncated — hit max_tokens limit");
        }

        if (ai.hasToolExecutionRequests())
        {
            for (ToolExecutionRequest req : ai.toolExecutionRequests())
            {
                DebugCollector.record("⚙ " + req.name() + "(" + req.arguments() + ")");
            }
        }
    }

    @Override
    public void onError(ChatModelErrorContext ctx) {}

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    private static void recordPrompt(List<ChatMessage> messages)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("📋 Augmented prompt (").append(messages.size()).append(" message(s)):\n");

        for (ChatMessage msg : messages)
        {
            if (msg instanceof SystemMessage sm)
            {
                sb.append("\n[SYSTEM]\n").append(truncate(sm.text()));
            }
            else if (msg instanceof UserMessage um)
            {
                // Only record the last user message — it's the one that contains
                // the injected Sefaria context.  Earlier user messages in the
                // chat history are omitted to keep the output focused.
                if (isLastUserMessage(msg, messages))
                {
                    sb.append("\n[USER — augmented]\n").append(truncate(um.singleText()));
                }
                else
                {
                    sb.append("\n[USER — history, omitted]");
                }
            }
            else if (msg instanceof AiMessage am)
            {
                sb.append("\n[ASSISTANT — history, omitted]");
            }
        }

        DebugCollector.record(sb.toString());
    }

    private static boolean isLastUserMessage(ChatMessage target, List<ChatMessage> messages)
    {
        ChatMessage lastUser = null;
        for (ChatMessage m : messages)
        {
            if (m instanceof UserMessage)
            {
                lastUser = m;
            }
        }
        return target == lastUser;
    }

    private static String truncate(String s)
    {
        return s != null ? s : "(null)";
    }
}
