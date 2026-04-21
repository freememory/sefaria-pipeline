package org.freememory.pipeline.agent.debug;

import dev.langchain4j.agent.tool.ToolExecutionRequest;
import dev.langchain4j.data.message.ChatMessage;
import dev.langchain4j.data.message.ToolExecutionResultMessage;
import dev.langchain4j.model.chat.listener.ChatModelErrorContext;
import dev.langchain4j.model.chat.listener.ChatModelListener;
import dev.langchain4j.model.chat.listener.ChatModelRequestContext;
import dev.langchain4j.model.chat.listener.ChatModelResponseContext;

/**
 * Intercepts LangChain4j chat model calls and records tool activity to
 * {@link DebugCollector} so the HTTP response can include a debug trace.
 *
 * Two events are captured per tool invocation:
 *
 *   ⚙ toolName({"arg":"value"})          — the LLM requested this tool call
 *   ↩ toolName → result text (truncated) — the tool's return value
 *
 * The listener is attached to all models (leaf + router) but the router
 * never issues tool calls, so it contributes nothing to the debug output.
 */
public class DebugChatModelListener implements ChatModelListener
{
    private static final int MAX_RESULT_LEN = 600;

    /** Called just before each LLM request — captures tool results from previous turns. */
    @Override
    public void onRequest(ChatModelRequestContext ctx)
    {
        for (ChatMessage msg : ctx.chatRequest().messages())
        {
            if (msg instanceof ToolExecutionResultMessage result)
            {
                String text = result.text();
                if (text != null && text.length() > MAX_RESULT_LEN)
                {
                    text = text.substring(0, MAX_RESULT_LEN) + "…";
                }
                DebugCollector.record("↩ " + result.toolName() + " → " + text);
            }
        }
    }

    /** Called just after each LLM response — captures tool call requests. */
    @Override
    public void onResponse(ChatModelResponseContext ctx)
    {
        var ai = ctx.chatResponse().aiMessage();
        if (ai.hasToolExecutionRequests())
        {
            for (ToolExecutionRequest req : ai.toolExecutionRequests())
            {
                DebugCollector.record("⚙ " + req.name() + "(" + req.arguments() + ")");
            }
        }
    }

    @Override
    public void onError(ChatModelErrorContext ctx)
    {
        // Not surfaced in debug output — errors appear in the response JSON's
        // error field and in server logs.
    }
}
