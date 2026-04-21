package org.freememory.pipeline.agent;

import dev.langchain4j.model.chat.ChatModel;
import dev.langchain4j.model.input.PromptTemplate;
import dev.langchain4j.rag.DefaultRetrievalAugmentor;
import dev.langchain4j.rag.content.injector.DefaultContentInjector;
import dev.langchain4j.rag.content.retriever.ContentRetriever;
import dev.langchain4j.service.AiServices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * A leaf node in the agent tree — performs the actual retrieval + LLM call.
 *
 * === Setup (once at startup) ===
 *
 * Each leaf holds a LangChain4j {@link AiServices}-built {@link GenericLeafAgent}
 * wired with:
 *   - A {@link ChatModel} (provider/model from config — e.g. Claude Sonnet)
 *   - A {@link SefariaRetriever} configured with this agent's category/language
 *     filters and topK value — automatically injects relevant Sefaria passages
 *   - A per-leaf {@link dev.langchain4j.memory.ChatMemory} stored in
 *     {@link ConversationContext} — provides multi-turn conversation history
 *   - Zero or more built-in {@code @Tool} providers (e.g. ZmanimTools)
 *   - Zero or more MCP tool providers (external MCP servers)
 *
 * === Per-call flow ===
 *
 * 1. LangChain4j retrieves relevant Sefaria passages via SefariaRetriever and
 *    injects them into the user message context.
 * 2. The dynamic system prompt (loaded from a classpath .txt resource) is
 *    expanded via the {@code {{systemPrompt}}} template variable.
 * 3. The full message chain — system prompt, chat history, augmented user
 *    message — is sent to the configured LLM.
 * 4. If any @Tool / MCP tool calls are generated, LangChain4j executes the
 *    agentic loop automatically and includes results before the final reply.
 * 5. The user+assistant turn is saved to the leaf's chat memory.
 */
public class LeafNode implements AgentNode
{
    private static final Logger log = LoggerFactory.getLogger(LeafNode.class);

    /**
     * Replaces LangChain4j's default "Answer using the following information:" framing.
     *
     * The default template makes the LLM believe the user supplied the retrieved texts,
     * which causes it to say things like "based on the texts you provided."  This template
     * makes the source of the context explicit: the system retrieved it automatically from
     * Sefaria, the user never saw it.
     */
    private static final PromptTemplate SEFARIA_CONTEXT_TEMPLATE = PromptTemplate.from(
            "{{userMessage}}\n\n" +
            "---\n" +
            "The following passages were automatically retrieved from the Sefaria database " +
            "by the system. The user did not provide these texts and is not aware of their " +
            "exact content. Use them as supporting evidence if they are relevant to the " +
            "question; if they are not relevant, answer from your own knowledge instead.\n\n" +
            "{{contents}}\n" +
            "---"
    );

    private final String           name;
    private final String           description;
    private final String           systemPrompt;
    private final GenericLeafAgent aiService;

    /**
     * @param name         display name (matches config node name)
     * @param description  one-line speciality description for parent routers
     * @param systemPrompt full system prompt text loaded from the classpath resource
     * @param model        chat model for this leaf (may differ from the router model)
     * @param retriever    Qdrant-backed retriever with this leaf's filter config
     * @param tools        built-in @Tool provider objects (may be empty)
     * @param mcpTools     MCP tool provider objects (may be empty)
     * @param chatMemorySize max turns to retain in the leaf's chat memory
     */
    public LeafNode(String name,
                    String description,
                    String systemPrompt,
                    ChatModel model,
                    ContentRetriever retriever,
                    List<Object> tools,
                    List<Object> mcpTools,
                    int chatMemorySize)
    {
        this.name         = name;
        this.description  = description;
        this.systemPrompt = systemPrompt;

        var augmentor = DefaultRetrievalAugmentor.builder()
                .contentRetriever(retriever)
                .contentInjector(new DefaultContentInjector(SEFARIA_CONTEXT_TEMPLATE))
                .build();

        var builder = AiServices.builder(GenericLeafAgent.class)
                .chatModel(model)
                .retrievalAugmentor(augmentor)
                .chatMemoryProvider(id -> {
                    // Memory key is the leaf name — each leaf has exactly one
                    // memory for the lifetime of this process (single-user CLI).
                    // Replace with a session-scoped ChatMemoryProvider for
                    // multi-user deployments.
                    return new dev.langchain4j.memory.chat.MessageWindowChatMemory
                            .Builder()
                            .maxMessages(chatMemorySize)
                            .build();
                });

        // Register built-in @Tool providers
        if (!tools.isEmpty())
        {
            builder.tools(tools);
        }

        // Register MCP tool providers
        for (Object mcpTool : mcpTools)
        {
            builder.toolProvider(
                    (dev.langchain4j.service.tool.ToolProvider) mcpTool);
        }

        this.aiService = builder.build();
        log.info("LeafNode '{}' initialised — tools: {}, mcp providers: {}",
                name,
                tools.stream()
                        .map(t -> t.getClass().getSimpleName())
                        .toList(),
                mcpTools.size());
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
        log.debug("[{}] Handling message: {}",
                name, userMessage.length() > 80
                        ? userMessage.substring(0, 80) + "…" : userMessage);

        return aiService.chat(userMessage, systemPrompt, ctx.getSessionId());
    }
}
