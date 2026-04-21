# Agent Architecture — Deep Dive

This document covers the internals of Phase 4 in detail: how the agent tree is
built from config, how each call flows through the tree, how retrieval and tool
calling work, and how to extend the system without touching any Java code.

---

## Table of Contents

1. [Overview](#overview)
2. [The Agent Tree Pattern](#the-agent-tree-pattern)
3. [AgentNode Interface](#agentnode-interface)
4. [RouterNode — Classifying and Delegating](#routernode--classifying-and-delegating)
5. [LeafNode — The Actual AI Call](#leafnode--the-actual-ai-call)
6. [GenericLeafAgent — The LangChain4j Service Interface](#genericleafagent--the-langchain4j-service-interface)
7. [ConversationContext — Session State](#conversationcontext--session-state)
8. [SefariaRetriever — RAG from Qdrant](#sefariaretriever--rag-from-qdrant)
9. [ModelFactory — Multi-Provider Model Resolution](#modelfactory--multi-provider-model-resolution)
10. [ToolRegistry — Built-in @Tool Methods](#toolregistry--built-in-tool-methods)
11. [MCP Tool Providers](#mcp-tool-providers)
12. [AgentTree — Building the Tree at Startup](#agenttree--building-the-tree-at-startup)
13. [A Question Traced End-to-End](#a-question-traced-end-to-end)
14. [Extending the System](#extending-the-system)

---

## Overview

The agent layer is a tree of nodes. Two node types exist:

| Type | Class | Responsibility |
|---|---|---|
| Router | `RouterNode` | Classify the user's question; delegate to the most suitable child |
| Leaf | `LeafNode` | Retrieve relevant Sefaria passages from Qdrant; call the LLM with full context |

The entire tree structure, every model choice, and every retrieval filter are
configured in `config/pipeline-agent.json` — adding or removing agents requires
only a JSON edit and optionally a new `.txt` system prompt file.

```
Sefaria Agent (router — claude-haiku-3-5)
├── Halakha (router — claude-haiku-3-5)
│   ├── Shabbat (leaf — claude-sonnet-4-6)
│   ├── Kashrut (leaf — claude-sonnet-4-6)
│   └── General Halakha (leaf — claude-sonnet-4-6)
├── Dvar Torah (leaf — claude-sonnet-4-6)
└── General Jewish Learning (leaf — claude-sonnet-4-6)
```

---

## The Agent Tree Pattern

Each node in the tree serves a single purpose: route or answer.

**Why a tree and not a flat list?**
A single router listing every possible agent becomes exponentially harder to
maintain as the number of agents grows. A two-level tree (meta-router → domain
router → specialist) keeps each routing decision small: the meta-router only
distinguishes *law vs. text vs. general*; the Halakha router only distinguishes
*Shabbat vs. Kashrut vs. general law*. Each classification prompt is tightly
scoped and accurate.

The tree also controls **cost**. Routing uses the cheapest capable model (Haiku,
~$0.0001/call). The heavyweight reasoning only fires at the leaf, where it is
actually needed.

---

## AgentNode Interface

```
src/main/java/org/sefaria/pipeline/agent/AgentNode.java
```

```java
public interface AgentNode {
    String getName();
    String getDescription();
    String handle(ConversationContext ctx, String userMessage);
}
```

Both `RouterNode` and `LeafNode` implement this interface. The `handle` method
accepts a `ConversationContext` (shared across the whole call) and returns a
fully-formed answer string.

`getDescription()` is used by parent routers when building their routing prompt —
each child's description is listed so the routing LLM understands what domain
each child covers.

---

## RouterNode — Classifying and Delegating

```
src/main/java/org/sefaria/pipeline/agent/RouterNode.java
```

### What it holds

- A lightweight `ChatModel` (the `routerModel` from config, e.g. `claude-haiku-3-5`)
- An ordered list of child `AgentNode` objects

### How routing works

On each `handle()` call, `RouterNode`:

1. **Builds a routing prompt** that lists every child's name and description:

   ```
   You are a routing assistant for a Jewish learning AI system.
   Read the user's question and select the most appropriate agent.

   Available agents:
     - Shabbat: Laws of Shabbat: melachot, candle lighting, eruv, muktzeh, electric devices
     - Kashrut: Laws of kosher food, meat and milk, slaughter, wine
     - General Halakha: All other Jewish law questions: prayer, lifecycle, ...

   Respond with ONLY the exact name of the chosen agent...
   ```

2. **Calls the routing model** with this system prompt and the user's message.
   The model replies with only the agent name — no explanation.

3. **Matches the response** to a child node:
   - Exact match (case-insensitive): preferred
   - Fuzzy match: partial substring in either direction — handles common LLM formatting noise
     like trailing punctuation or extra words
   - **Fallback**: if no match, log a warning and delegate to the first child
     (which should be a sensible general-purpose fallback)

4. **Records the routing decision** in `ConversationContext` for debugging
   (`/path` command in the CLI shows the full routing chain).

5. **Recursively calls `handle()`** on the chosen child, passing the same
   `ConversationContext` and the original user message unchanged.

### Why no memory for routers?

Routing is a classification task over a single message. Injecting prior conversation
history into the routing prompt would increase cost and add noise without
benefiting the decision. Only leaves need memory.

---

## LeafNode — The Actual AI Call

```
src/main/java/org/sefaria/pipeline/agent/LeafNode.java
```

### What it holds

Each leaf owns one fully-wired `GenericLeafAgent` instance (a LangChain4j
AI Service proxy), constructed once at startup. The proxy encapsulates:

- A `ChatModel` (the full-size model from config, or a per-leaf override)
- A `SefariaRetriever` configured with this leaf's category/language/topK filters
- A per-leaf `MessageWindowChatMemory` (sliding window of last N turns)
- Zero or more built-in `@Tool` provider objects
- Zero or more MCP `ToolProvider` objects

### Per-call flow

When `handle()` is called:

```
LeafNode.handle()
    │
    ▼
GenericLeafAgent.chat(userMessage, systemPrompt)
    │
    │  LangChain4j intercepts this call:
    │
    ├── 1. SefariaRetriever.retrieve(query)
    │       │
    │       ├── Embed the user message
    │       ├── Search Qdrant with category/language/primaryOnly filters
    │       └── Return top-K formatted citation blocks
    │
    ├── 2. Inject citations into user message context
    │
    ├── 3. Expand {{systemPrompt}} template variable
    │
    ├── 4. Build message chain:
    │       system prompt + chat history + augmented user message
    │
    ├── 5. Call the LLM
    │
    ├── 6. If LLM requests tool calls:
    │       ├── Execute each @Tool / MCP tool
    │       ├── Return result to LLM
    │       └── Repeat until no more tool calls
    │
    └── 7. Save user+assistant turn to chat memory
         Return final text answer
```

### Chat memory scope

Each leaf has exactly one `ChatMemory` instance for the lifetime of the process
(single-user CLI). When routing switches from Shabbat to Kashrut and back to
Shabbat, the Shabbat leaf retains its earlier history. The Kashrut leaf starts
fresh the first time it receives a message in a session.

For a multi-user deployment, replace the single-instance design with a
session-ID-keyed `ChatMemoryProvider` — LangChain4j's `chatMemoryProvider()`
builder method accepts a function from session ID to `ChatMemory`.

---

## GenericLeafAgent — The LangChain4j Service Interface

```
src/main/java/org/sefaria/pipeline/agent/GenericLeafAgent.java
```

```java
interface GenericLeafAgent {
    @SystemMessage("{{systemPrompt}}")
    String chat(@UserMessage String message, @V("systemPrompt") String systemPrompt);
}
```

This is the core design choice for extensibility. Instead of one interface per
agent type (which would require code changes for every new specialist), a single
interface with a **template variable** system prompt lets all leaves share the
same interface while receiving completely different behaviour at runtime.

The `@SystemMessage("{{systemPrompt}}")` annotation tells LangChain4j that the
system message is a Mustache template. The `@V("systemPrompt")` parameter
supplies the value at call time — in this case, the text loaded from the leaf's
`prompts/*.txt` resource file.

The proxy built by `AiServices.builder(GenericLeafAgent.class)...build()` is a
JDK dynamic proxy. At runtime, when `chat()` is called, LangChain4j:

1. Expands `{{systemPrompt}}` with the argument value
2. Prepends any retrieved RAG content to the user message
3. Injects chat history
4. Sends to the model, runs the tool loop if needed
5. Updates chat memory
6. Returns the final string

From the Java caller's perspective it is just a method call.

---

## ConversationContext — Session State

```
src/main/java/org/sefaria/pipeline/agent/ConversationContext.java
```

A `ConversationContext` is created once per user conversation (one per call to
`RunAgentScript`'s `/new` or at startup) and passed down through the entire
`handle()` call chain.

### What it tracks

**Per-leaf chat memories** (`Map<String, ChatMemory>`):
```java
public ChatMemory getMemoryFor(String leafName) {
    return leafMemories.computeIfAbsent(
        leafName, k -> MessageWindowChatMemory.withMaxMessages(memorySize)
    );
}
```
Lazily created on first access. In `LeafNode`, the `chatMemoryProvider` lambda
uses this:
```java
.chatMemoryProvider(id -> ctx.getMemoryFor(name))
```
The `id` argument from LangChain4j is the conversation ID — in single-user CLI
mode we ignore it and key on the agent's name instead.

**Routing path log** (`List<String>`):
Each `RouterNode.handle()` call appends `"RouterName → ChildName"` to the log.
`getLastRoutingPath()` returns the most recent entry. The CLI `/path` command
calls this to show the chain for the last question:

```
Agent [Sefaria Agent → Halakha → Shabbat]:
```

---

## SefariaRetriever — RAG from Qdrant

```
src/main/java/org/sefaria/pipeline/agent/SefariaRetriever.java
```

Implements LangChain4j's `ContentRetriever` interface:
```java
public interface ContentRetriever {
    List<Content> retrieve(Query query);
}
```

When LangChain4j calls `retrieve()` before each LLM call:

### Step 1 — Embed the query

```java
Embedding queryEmbedding = embeddingModel.embed(queryText).content();
float[] vector = queryEmbedding.vector();
```

Uses the same `text-embedding-3-small` model used at ingest time. The query and
all stored chunks exist in the same 1536-dimensional vector space, so cosine
similarity produces meaningful results.

### Step 2 — Build the Qdrant filter

`buildFilter()` constructs a Qdrant protobuf `Filter` from the leaf's
`RetrievalConfig`:

| Config field | Qdrant filter |
|---|---|
| `categories: ["Halakhah", "Talmud"]` | `should` (OR): `category = "Halakhah"` OR `category = "Talmud"` |
| `language: "en"` | `must`: `language = "en"` |
| `primaryOnly: true` | `must`: `is_primary = true` |
| `categories: []` (empty) | No category filter — full collection |

All active filters are joined into a `must` clause (AND). The category OR-group
is wrapped in a nested filter object, which Qdrant evaluates efficiently because
`category` is a payload index.

### Step 3 — Execute the vector search

```java
results = qdrant.searchAsync(search.build()).get();
```

Returns up to `topK` scored points, each with its full payload.

### Step 4 — Format citation blocks

Each point is formatted as a citation header followed by the chunk text:

```
[Shabbat 2a | Talmud Bavli | Talmud | en]
Carrying in a public domain is one of the 39 principal melachot…
```

These citation blocks are injected verbatim into the LLM's context. The system
prompt for each leaf instructs the model to cite these references in its answer.

---

## ModelFactory — Multi-Provider Model Resolution

```
src/main/java/org/sefaria/pipeline/agent/ModelFactory.java
```

`ModelFactory.create(ModelConfig cfg, AgentConfig agentCfg)` returns a
`ChatModel` for any supported provider.

### Supported providers

| `provider` value | LangChain4j class | API key env var fallback |
|---|---|---|
| `anthropic` | `AnthropicChatModel` | `ANTHROPIC_API_KEY` |
| `openai` | `OpenAiChatModel` | `OPENAI_API_KEY` |
| `google` | `GoogleAiGeminiChatModel` | `GOOGLE_API_KEY` |
| `mistral` | `MistralAiChatModel` | `MISTRAL_API_KEY` |
| `ollama` | `OllamaChatModel` | none (uses `baseUrl`) |

### API key resolution

`agentCfg.resolveApiKey(provider)` checks:
1. `agent.providers.<name>.apiKey` in the config file
2. The provider-specific environment variable

The config-file key takes priority. This is intentional: for local development
you set keys in your config file (never committed); in CI/CD you set environment
variables.

### Per-node model overrides

Model resolution cascades through the tree:

```
agent.defaultModel          ← fallback for all leaves
  └── node.model            ← per-node override (optional)
agent.routerModel           ← always used by RouterNode
```

This lets you run most leaves on Claude Sonnet while routing on Haiku, and
optionally send one expensive specialist (e.g. a Talmud commentary agent) to
GPT-4o or a local Ollama model for cost control:

```json
{
  "name": "TalmudCommentary",
  "type": "leaf",
  "model": { "provider": "ollama", "modelId": "llama3.2" },
  ...
}
```

---

## ToolRegistry — Built-in @Tool Methods

```
src/main/java/org/sefaria/pipeline/agent/ToolRegistry.java
```

Maps the tool name strings from config (e.g. `"ZMANIM"`) to live Java objects.
Each tool object has one or more `@Tool`-annotated methods that LangChain4j
registers as callable functions on the LLM.

### Built-in tools

| Name | Class | What it does |
|---|---|---|
| `ZMANIM` | `ZmanimTools` | Fetches halachic times (sunrise, sunset, candle lighting) from the Hebcal REST API |
| `HEBREW_CALENDAR` | `HebrewCalendarTools` | Parasha of the week, holidays, Hebrew↔Gregorian date conversion via Hebcal |
| `SEFARIA_LOOKUP` | `SefariaLookupTools` | Fetches the exact Sefaria chunk for a known ref string by Qdrant payload scroll |

### How @Tool works at the LLM level

LangChain4j converts each `@Tool`-annotated method into an OpenAI/Anthropic
function specification describing the method name, description, and parameters.
This spec is included in the LLM API call. When the model decides a tool is
needed, it returns a structured call request instead of text. LangChain4j
executes the Java method, injects the return value as a tool result message, and
sends the augmented conversation back to the LLM. The model may call tools zero,
one, or many times before producing a final text response.

Example interaction:

```
User: "What time is candle lighting in Tel Aviv this Friday?"

LLM → tool call: getShabbatTimes(city="Tel Aviv")
Java → Hebcal API → "Candle lighting: 19:07"
LLM → tool call: getHebrewDate(gregorianDate="2024-11-22")  ← (optional, model decides)
Java → Hebcal API → "25 Cheshvan 5785"

LLM final answer:
"Candle lighting in Tel Aviv this Friday (25 Cheshvan 5785) is at 7:07 PM."
```

### Adding a new built-in tool

1. Create `src/.../agent/tools/MyTools.java`:

   ```java
   public class MyTools {
       @Tool("Description shown to the LLM — be specific and functional")
       public String myMethod(
               @P("Parameter description") String param) {
           // Make HTTP call, DB query, calculation — any Java code
           return "result string the LLM will read";
       }
   }
   ```

2. Add a constant and case in `ToolRegistry`:

   ```java
   public static final String MY_TOOL = "MY_TOOL";

   // in instantiate():
   case MY_TOOL -> new MyTools();
   ```

3. Reference it in the config:

   ```json
   "tools": ["MY_TOOL"]
   ```

---

## MCP Tool Providers

MCP (Model Context Protocol) is a standard for connecting LLMs to external
**tool servers** — separate processes that expose tools over HTTP/SSE or stdio,
in any language.

From the LLM's perspective, MCP tools are indistinguishable from `@Tool` Java
methods. LangChain4j's `McpToolProvider` handles the protocol: it discovers
available tools from the server at startup, converts them to function
specifications, proxies calls to the server during the agentic loop, and returns
results.

### Transport modes

**HTTP/SSE** (`url` field in config):
```json
"mcpServers": [
  { "key": "hebcal", "url": "http://localhost:3001/mcp" }
]
```
Uses `HttpMcpTransport` (Streamable HTTP). The server must be running separately.

**Stdio subprocess** (`command` field in config):
```json
"mcpServers": [
  { "key": "filesystem",
    "command": ["npx", "@modelcontextprotocol/server-filesystem", "/data"] }
]
```
Uses `StdioMcpTransport`. The JVM spawns the process on startup, communicates
over stdin/stdout, and owns the process lifecycle.

### Example: connecting a Hebcal MCP server

If a public or self-hosted Hebcal MCP server is available at port 3001:

```json
{
  "name": "Shabbat",
  "type": "leaf",
  "mcpServers": [
    { "key": "hebcal", "url": "http://localhost:3001/mcp" }
  ]
}
```

At startup, `AgentTree` connects the transport, calls the server's tool
discovery endpoint, and registers all discovered tools on the Shabbat leaf's
`AiServices` instance. No code changes needed.

### MCP vs. @Tool: when to use each

| Situation | Use |
|---|---|
| Simple HTTP API call, 1–3 endpoints | `@Tool` Java method |
| Complex tool server with many tools | MCP |
| Third-party tool server you don't control | MCP |
| Tool requires non-Java runtime (Python, Node) | MCP stdio |
| You want tool discovery at startup, not compile time | MCP |

---

## AgentTree — Building the Tree at Startup

```
src/main/java/org/sefaria/pipeline/agent/AgentTree.java
```

`AgentTree.build()` is called once by `RunAgentScript` to construct the entire
tree. It walks the config recursively:

```
build()
  └── buildNode(treeConfig, defaultModel)
        ├── if type=router → buildRouter()
        │     ├── create ChatModel from routerModel config
        │     └── recurse: buildNode() for each child
        └── if type=leaf → buildLeaf()
              ├── create ChatModel from effectiveModel
              ├── create SefariaRetriever from retrieval config
              ├── resolve built-in tools via ToolRegistry
              ├── connect MCP servers via buildMcpProviders()
              └── load system prompt from classpath resource
```

### System prompt loading

`loadPrompt(promptFile, agentName)` reads from the classpath:
```
src/main/resources/prompts/halakha_shabbat.txt
```

If the resource is not found, a minimal default prompt is generated from the
agent's name. The fallback is intentional: a new agent works immediately even
before its bespoke prompt is written, and startup is never blocked by a missing
file.

### Model inheritance

`effectiveModel` flows down the recursion:

```
build() passes agentConfig.defaultModel as inheritedModel
  ↓
buildNode() checks: does this node have its own "model" override?
  ├── yes → use node.model
  └── no  → use inheritedModel
       ↓
       pass as inheritedModel to children
```

Routers always use `agentConfig.getRouterModel()` regardless of inheritance —
routing is a different task from answering and should always use the fast,
cheap model.

---

## A Question Traced End-to-End

**Question:** *"Is it permitted to use a timer on Shabbat to turn on lights?"*

### 1. RunAgentScript receives the question

Creates a fresh `ConversationContext(chatMemorySize=20)` for this session (or
reuses the existing one for follow-up questions). Calls `root.handle(ctx, q)`.

### 2. Sefaria Agent router classifies

Routing prompt lists three children: Halakha, Dvar Torah, General Jewish
Learning. Sends to `claude-haiku-3-5`. Model responds: `"Halakha"`.

`ctx.recordRouting("Sefaria Agent", "Halakha")`. Calls `halakha.handle(ctx, q)`.

### 3. Halakha router classifies

Routing prompt lists three children: Shabbat, Kashrut, General Halakha. Model
responds: `"Shabbat"`.

`ctx.recordRouting("Halakha", "Shabbat")`. Calls `shabbat.handle(ctx, q)`.

### 4. Shabbat leaf handles

Calls `aiService.chat(question, shabbatSystemPrompt)`. LangChain4j intercepts:

**Retrieval:** `SefariaRetriever` embeds `"Is it permitted to use a timer on Shabbat to turn on lights?"` → calls Qdrant with filter `category IN ["Halakhah","Talmud","Mishnah"]` → returns top 20 chunks including:

```
[Shulchan Aruch, Orach Chayim 276:4 | Shulchan Aruch | Halakhah | en]
It is permissible to benefit from a candle lit by a non-Jew for a Jew...

[Mishnah Berurah 276:16 | Mishnah Berurah | Halakhah | en]
The Acharonim discuss the use of a clock mechanism set before Shabbat...

[Shabbat 17b | Talmud Bavli | Talmud | en]
One may not set a trap before Shabbat unless it will close...
```

**Tool call (optional):** The system prompt says to check current Shabbat times
when relevant. The model decides to call `getShabbatTimes("Jerusalem")` via
`ZmanimTools` to give a time-anchored answer. Java calls Hebcal API, returns
candle lighting and Havdalah times.

**LLM call:** System prompt (Shabbat specialist with Shulchan Aruch citation
instructions) + 20 Sefaria chunks + tool result + user question → `claude-sonnet-4-6`.

**Answer returned:**
> *"Using a timer (שעון שבת) set before Shabbat to turn lights on or off is
> generally permitted by most Acharonim, based on the principle that a
> pre-programmed mechanism acting automatically is not considered a Shabbat
> violation. See Shulchan Aruch OC 276:4 and the Mishnah Berurah 276:16 for the
> underlying reasoning. However, some authorities distinguish between turning
> lights on (permitted) and off (more lenient opinions but some dispute). Check
> with your posek for a ruling in your specific situation."*

### 5. Response returned to user

`RunAgentScript` prints:
```
Agent [Sefaria Agent → Halakha → Shabbat]:
Using a timer (שעון שבת) set before Shabbat...
```

The user+assistant turn is saved to the Shabbat leaf's `ChatMemory`.

---

## Extending the System

### Add a leaf agent (no code)

1. Add a JSON node to `pipeline-agent.json` under the appropriate router's `children`:

   ```json
   {
     "name": "Tefillah",
     "description": "Laws and meanings of Jewish prayer, siddur, daily davening",
     "type": "leaf",
     "promptFile": "prompts/halakha_tefillah.txt",
     "tools": ["HEBREW_CALENDAR"],
     "retrieval": {
       "categories": ["Halakhah", "Talmud", "Liturgy"],
       "language": null,
       "topK": 20,
       "primaryOnly": false
     }
   }
   ```

2. Create `src/main/resources/prompts/halakha_tefillah.txt` with the specialist
   system prompt. That's it — rebuild and run.

### Add a sub-router

Wrap leaves in a router node. The parent router automatically lists the new
sub-router's description as a routing option:

```json
{
  "name": "Daily Life",
  "description": "Questions about prayer, blessings, mezuzah, and domestic halacha",
  "type": "router",
  "children": [
    { "name": "Tefillah", "type": "leaf", ... },
    { "name": "Brachot",  "type": "leaf", ... },
    { "name": "Mezuzah",  "type": "leaf", ... }
  ]
}
```

### Override a model for one node

```json
{
  "name": "Kabbalah",
  "type": "leaf",
  "model": { "provider": "openai", "modelId": "gpt-4o" },
  ...
}
```

### Add an MCP tool server

```json
{
  "name": "Shabbat",
  "type": "leaf",
  "mcpServers": [
    { "key": "zmanim-server", "url": "http://localhost:4000/mcp" }
  ],
  ...
}
```

Start the MCP server separately; the agent connects automatically at startup.

### Add a built-in @Tool

1. Create `src/.../agent/tools/MyTools.java` with `@Tool` methods.
2. Add a constant and `case` in `ToolRegistry.instantiate()`.
3. Add the tool name to the relevant leaf's `tools` array in config.
