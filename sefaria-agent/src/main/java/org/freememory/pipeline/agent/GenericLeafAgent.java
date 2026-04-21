package org.freememory.pipeline.agent;

import dev.langchain4j.service.MemoryId;
import dev.langchain4j.service.SystemMessage;
import dev.langchain4j.service.UserMessage;
import dev.langchain4j.service.V;

/**
 * LangChain4j AI Service interface used by every leaf node in the agent tree.
 *
 * Using a single interface with a dynamic {@code {{systemPrompt}}} variable
 * means each leaf can have its own persona and instructions (loaded from a
 * classpath resource) without requiring a separate Java interface per agent.
 *
 * The {@code @MemoryId} parameter lets each HTTP session (identified by a UUID)
 * maintain its own conversation history at every leaf.  In CLI mode the session
 * ID is the constant {@code "default"}, giving a single shared memory — the
 * same behaviour as before.
 */
interface GenericLeafAgent
{
    @SystemMessage("{{systemPrompt}}")
    String chat(@UserMessage  String message,
                @V("systemPrompt") String systemPrompt,
                @MemoryId    String sessionId);
}
