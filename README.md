# Sefaria Pipeline

A four-phase Java pipeline that downloads the entire [Sefaria](https://www.sefaria.org) Jewish text corpus, processes it into semantically coherent chunks, embeds it into a vector database, and powers an AI agent capable of answering questions about Torah, Talmud, Halacha, and Jewish thought — with full citations.

---

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Quick Start](#quick-start)
3. [Project Structure](#project-structure)
4. [Configuration](#configuration)
5. [The Four Phases](#the-four-phases)
6. [Key Concepts for Java Developers New to AI](#key-concepts-for-java-developers-new-to-ai)
7. [LangChain4j — What It Is and What It Does](#langchain4j--what-it-is-and-what-it-does)
8. [The Agent Architecture](#the-agent-architecture)
9. [Tools and MCP](#tools-and-mcp)
10. [Adding New Agents](#adding-new-agents)
11. [Cost and Scale Estimates](#cost-and-scale-estimates)

---

## Prerequisites

| Requirement | Version | Notes |
|---|---|---|
| Java | 21+ | Virtual threads, switch expressions |
| Maven | 3.9+ | |
| Docker | any | Used to run Qdrant locally |
| OpenAI API key | — | For embeddings (`text-embedding-3-small`) |
| Anthropic API key | — | For the agent LLM (Claude) |

**Start Qdrant:**
```bash
docker run -d -p 6333:6333 -p 6334:6334 \
    -v /path/to/qdrant_storage:/qdrant/storage \
    qdrant/qdrant
```

---

## Quick Start

```bash
# Build
mvn package -DskipTests

# Phase 1: download Sefaria texts (P0 = Tanakh, Talmud, Mishnah, Halacha)
java -cp target/sefaria-pipeline.jar org.freememory.scripts.DownloadScript \
     --config config/pipeline.json

# Phase 2: process raw JSON into chunks
java -cp target/sefaria-pipeline.jar org.freememory.scripts.ProcessScript \
     --config config/pipeline.json

# Phase 3: embed chunks and ingest into Qdrant
java -cp target/sefaria-pipeline.jar org.freememory.scripts.EmbedIngestScript \
     --config config/pipeline-full.json

# Phase 4: run the agent
java -cp target/sefaria-pipeline.jar org.freememory.scripts.RunAgentScript \
     --config config/pipeline-agent.json
```

---

## Project Structure

```
sefaria-pipeline/
  config/
    pipeline.json           # P0 texts (Tanakh, Talmud, Mishnah, Halacha)
    pipeline-full.json      # Full corpus — all categories
    pipeline-agent.json     # Agent tree configuration ← edit this for Phase 4
  docs/
    AGENT.md                # Deep-dive on the agent architecture
  src/main/java/org/sefaria/
    config/
      PipelineConfig.java   # All configuration classes (one class per phase)
      ConfigLoader.java     # Loads JSON config from --config flag or default path
    pipeline/
      download/             # Phase 1: HTTP download, schema fetch, links index
      process/              # Phase 2: HTML cleaning, chunking, ref building
      embed/                # Phase 3: OpenAI embedding, Qdrant upsert
      agent/                # Phase 4: agent tree (see docs/AGENT.md)
        tools/              # @Tool providers: zmanim, Hebrew calendar, Sefaria lookup
    scripts/                # Entrypoints: one main() per phase
  src/main/resources/
    prompts/                # System prompts for each leaf agent (plain text files)
  data/
    raw/                    # Downloaded Sefaria JSON (gitignored)
    schemas/                # Downloaded schema JSON (gitignored)
    links/                  # Downloaded links CSV (gitignored)
    links_index/links.db    # SQLite cross-reference index (gitignored)
    processed/              # JSONL chunk files (gitignored)
```

---

## Configuration

All phases share a single JSON config file. Each phase reads only its own section.

### API keys

Keys are read from the config file first; environment variables are the fallback.
**Recommended for local development:** put keys directly in your config file.
Never commit a config file with real keys to a public repository.

```json
"agent": {
  "providers": {
    "anthropic": { "apiKey": "sk-ant-YOUR-KEY-HERE" },
    "openai":    { "apiKey": "sk-YOUR-KEY-HERE" }
  }
}
```

The same pattern applies for Phase 3 embeddings, under `embed.openAiApiKey`.

Environment variable fallbacks (used when the config value is absent or blank):

| Provider | Env var |
|---|---|
| anthropic | `ANTHROPIC_API_KEY` |
| openai | `OPENAI_API_KEY` |
| google | `GOOGLE_API_KEY` |
| mistral | `MISTRAL_API_KEY` |

### Supported model providers

Set `provider` on any `defaultModel` or `routerModel` entry:

| Provider | `provider` value | Example `modelId` |
|---|---|---|
| Anthropic (Claude) | `anthropic` | `claude-sonnet-4-6`, `claude-haiku-3-5` |
| OpenAI | `openai` | `gpt-4o`, `gpt-4o-mini` |
| Google Gemini | `google` | `gemini-1.5-pro`, `gemini-1.5-flash` |
| Mistral | `mistral` | `mistral-large-latest` |
| Ollama (local) | `ollama` | `llama3.2`, `qwen2.5` |

---

## The Four Phases

### Phase 1 — Download (`DownloadScript`)

Downloads all Sefaria texts, schemas, and cross-reference links from the Sefaria GCS bucket.

- Reads `books.json` (the master catalog of ~19,643 texts)
- Downloads bilingual `merged.json` files (English + Hebrew) for each text
- Downloads `schemas/{Title}.json` (structural metadata per text)
- Downloads 16 `links/*.csv` files (~650 MB) and builds a SQLite index (`links.db`)
- Resume-safe: skips files already present on disk

Output: `data/raw/`, `data/schemas/`, `data/links_index/links.db`

### Phase 2 — Process (`ProcessScript`)

Turns raw JSON into JSONL chunk files ready for embedding.

- Strips HTML from text (Jsoup)
- Walks the jagged nested array structure of each text
- Builds canonical Sefaria reference strings ("Shabbat 2a:5", "Genesis 1:1")
- Applies the correct chunking strategy per text type (aliyah, amud, mishnah, prose...)
- Enriches each chunk with cross-reference links from `links.db`
- Writes one JSONL file per source text to `data/processed/`

Output: `data/processed/*.jsonl`

### Phase 3 — Embed + Ingest (`EmbedIngestScript`)

Embeds each chunk and stores it in Qdrant.

- Reads JSONL files line-by-line (streaming — files can be hundreds of MB)
- Calls OpenAI `text-embedding-3-small` in batches of 100 chunks
- Upserts each chunk as a Qdrant point (UUID derived from `chunk_id` SHA-256)
- Idempotent: re-runs skip already-ingested chunks at zero cost
- Adaptive rate-limit retry: parses OpenAI's "try again in Xs" and sleeps exactly that long

Output: Populated Qdrant collection `sefaria_texts`

### Phase 4 — Agent (`RunAgentScript`)

Interactive CLI powered by a tree of AI agents.

- Loads the agent tree from `config/pipeline-agent.json`
- Reads your question, routes it to the right specialist agent
- The specialist searches Qdrant for relevant passages, calls any tools needed, and generates a cited response
- Multi-turn conversation with per-agent chat memory

---

## Key Concepts for Java Developers New to AI

If you've never built an AI application before, these five concepts will make everything else click.

### 1. Tokens

LLMs don't read text as characters or words — they read **tokens**, which are roughly 3–4 characters of English each. The embedding model `text-embedding-3-small` accepts at most **8,191 tokens** per input. This is why Phase 2 chunks texts into ~300–450 token pieces rather than feeding whole books at once.

Hebrew text with nikkud (vowel-pointing marks) tokenizes at roughly **2 tokens per character** because each diacritic is a separate Unicode code point and often its own BPE (Byte Pair Encoding) token. This is why `MAX_CHARS_PER_CHUNK = 4,000` in `ChunkIngestor` — at a 2:1 ratio, 4,000 chars → 8,000 tokens, safely under the limit.

### 2. Embeddings and Vector Search

An **embedding** is a list of ~1,500 floating-point numbers (a *vector*) that encodes the *semantic meaning* of a piece of text. Two passages that discuss the same concept will have vectors pointing in similar directions in that 1,500-dimensional space — even if they use different words, or are in different languages.

```
"Is it permitted to carry on Shabbat?"  →  [0.023, -0.451, 0.178, ...]
"מותר לטלטל בשבת?"                      →  [0.021, -0.447, 0.181, ...]  ← very close
"Recipe for cholent"                    →  [-0.312, 0.089, -0.203, ...] ← far away
```

**Cosine similarity** measures the angle between two vectors. Vectors pointing in the same direction (cosine ≈ 1.0) are semantically similar.

**Qdrant** is a vector database. You insert vectors with attached metadata (payload), and query it with a new vector to get the *k* most similar back. That's how the agent finds relevant Sefaria passages for any question.

### 3. RAG — Retrieval-Augmented Generation

LLMs have a fixed knowledge cutoff and a finite context window. RAG solves both:

```
User question
     │
     ▼
Embed the question → query vector
     │
     ▼
Search Qdrant → top-20 most relevant Sefaria passages
     │
     ▼
Inject passages into the LLM context window
     │
     ▼
LLM generates a cited answer grounded in those specific passages
```

Without RAG, the LLM might hallucinate citations or give vague answers. With RAG, every claim is grounded in a real Sefaria passage that was retrieved and shown to the model. The model's job shifts from *remembering* to *reasoning*.

### 4. The System Prompt

Every LLM conversation has three message types:

- **System message**: Instructions about the model's role, tone, and constraints. Set once per session. The user never sees it directly.
- **User message**: What the human typed.
- **Assistant message**: What the model replied.

In this project, each leaf agent has its own system prompt loaded from a `.txt` file in `src/main/resources/prompts/`. The Shabbat agent's system prompt instructs it to cite Shulchan Aruch references; the Dvar Torah agent's tells it to draw on Midrash and commentators. Same underlying LLM, completely different behaviour.

### 5. Tool Calling / Function Calling

Modern LLMs can decide to call external functions mid-conversation. You describe a function to the model (name, description, parameter names and types), and the model emits a structured call request when it decides the function is needed. Your code executes it and returns the result; the model incorporates the result into its response.

```
User: "What time is candle lighting in Jerusalem tonight?"

LLM decides: "I need real-time zmanim data — I'll call getZmanim."
LLM emits:   { "tool": "getZmanim", "city": "Jerusalem", "date": "today" }

Java calls:  Hebcal API → JSON with times
Java returns result to LLM

LLM responds: "Candle lighting in Jerusalem tonight is at 6:43 PM."
```

In LangChain4j you annotate a Java method with `@Tool` and a description string. LangChain4j converts it into the tool specification the LLM expects, runs the call/response loop automatically, and feeds results back. The LLM can call tools zero, one, or many times before producing its final answer.

---

## LangChain4j — What It Is and What It Does

[LangChain4j](https://docs.langchain4j.dev) is a Java library providing abstractions for building LLM-powered applications. Think of it as a set of adapters and patterns that smooth over the differences between LLM providers and handle common application patterns (RAG, tool calling, memory) so you don't have to.

### Unified `ChatModel` interface

Without LangChain4j you'd POST directly to `api.anthropic.com`, parse JSON, handle retries, etc. With it:

```java
// One interface, many providers — swap freely
ChatModel claude  = AnthropicChatModel.builder().apiKey(key).modelName("claude-sonnet-4-6").build();
ChatModel gpt4    = OpenAiChatModel.builder().apiKey(key).modelName("gpt-4o").build();
ChatModel gemini  = GoogleAiGeminiChatModel.builder().apiKey(key).modelName("gemini-1.5-pro").build();
ChatModel llama   = OllamaChatModel.builder().baseUrl("http://localhost:11434").modelName("llama3.2").build();

// All have the same call pattern:
ChatResponse r = claude.chat(SystemMessage.from("..."), UserMessage.from("..."));
String text = r.aiMessage().text();
```

### AI Services — the annotation pattern

This is LangChain4j's most distinctive feature. You declare a Java *interface* annotated with LangChain4j annotations, and `AiServices.builder()` produces a proxy implementation at runtime — much like how Spring creates repository proxies from `@Repository` interfaces.

```java
// You write:
interface HalakhaAgent {
    @SystemMessage("You are a Halakhic scholar. Cite sources by Sefaria ref.")
    String ask(@UserMessage String question);
}

// LangChain4j builds a working implementation:
HalakhaAgent agent = AiServices.builder(HalakhaAgent.class)
    .chatModel(claude)
    .build();

// You call it like any Java object:
String answer = agent.ask("Is it permitted to use a timer on Shabbat?");
```

The proxy handles API construction, HTTP calls, response parsing, the tool-calling loop, memory, and RAG — all transparently.

This project uses a single generic interface `GenericLeafAgent` with a `{{systemPrompt}}` template variable so all leaf agents share the same interface but get different system prompts at runtime:

```java
interface GenericLeafAgent {
    @SystemMessage("{{systemPrompt}}")
    String chat(@UserMessage String message, @V("systemPrompt") String systemPrompt);
}
```

### Chat memory

```java
AiServices.builder(HalakhaAgent.class)
    .chatModel(claude)
    .chatMemory(MessageWindowChatMemory.withMaxMessages(20))
    .build();
```

LangChain4j maintains a sliding window of the last 20 user+assistant message pairs and injects them into every call, giving the LLM conversation context without you managing message history manually.

### Content retrieval (RAG)

```java
AiServices.builder(HalakhaAgent.class)
    .chatModel(claude)
    .contentRetriever(sefariaRetriever)  // your Qdrant-backed retriever
    .build();
```

LangChain4j calls `sefariaRetriever.retrieve(query)` before every LLM call, injects the returned passages into the user message context, and sends the augmented message to the model. The `ContentRetriever` interface is just:

```java
public interface ContentRetriever {
    List<Content> retrieve(Query query);
}
```

`SefariaRetriever` implements this by embedding the query, searching Qdrant with the agent's configured category/language filters, and returning formatted citation blocks.

### Tool calling

```java
class ZmanimTools {
    @Tool("Get halachic times (zmanim) for a city and date")
    public String getZmanim(@P("City name") String city, @P("Date YYYY-MM-DD") String date) {
        return callHebcalApi(city, date);  // real HTTP call
    }
}

AiServices.builder(HalakhaAgent.class)
    .chatModel(claude)
    .tools(new ZmanimTools())
    .build();
```

LangChain4j converts `@Tool` methods into tool specifications, handles the full agentic loop (send → tool call request → execute → send result → repeat until done → return), and injects results back into the conversation.

---

## The Agent Architecture

See [`docs/AGENT.md`](docs/AGENT.md) for the full deep-dive.

**The short version:** agents form a tree. Internal nodes are **routers** that classify questions and delegate to children. Leaf nodes are **specialists** with a domain system prompt, targeted Qdrant retrieval, and optional tools.

```
Sefaria Agent (router — cheap model, just classifies)
├── Halakha (router)
│   ├── Shabbat (leaf)       ← Shulchan Aruch prompt + [Halakhah,Talmud,Mishnah] filter + ZmanimTools
│   ├── Kashrut (leaf)       ← Kashrut prompt + [Halakhah,Talmud,Mishnah] filter
│   └── General Halakha (leaf)
├── Dvar Torah (leaf)        ← Dvar Torah prompt + [Tanakh,Midrash,Jewish Thought] filter
└── General Learning (leaf)
```

The entire tree is defined in `config/pipeline-agent.json`. Adding a new agent is a JSON edit and a `.txt` file — no code changes.

---

## Tools and MCP

### Built-in tools (`@Tool` Java methods)

| Config name | Class | What it calls |
|---|---|---|
| `ZMANIM` | `ZmanimTools` | Hebcal zmanim REST API — sunrise, sunset, candle lighting, etc. |
| `HEBREW_CALENDAR` | `HebrewCalendarTools` | Hebcal — parasha, holidays, Hebrew/Gregorian date conversion |
| `SEFARIA_LOOKUP` | `SefariaLookupTools` | Direct Qdrant lookup by Sefaria ref string |

**Adding a built-in tool:**
1. Create `src/.../agent/tools/MyTools.java` with `@Tool`-annotated methods
2. Add a constant and a `case` in `ToolRegistry`
3. Reference the name in the leaf's `"tools"` array in the config

### External MCP tools

MCP (Model Context Protocol) is a standard for connecting LLMs to external tool *servers*. The server is a separate process; the LLM sees its tools exactly as it would see `@Tool` Java methods.

```json
{
  "name": "Shabbat",
  "type": "leaf",
  "mcpServers": [
    { "key": "hebcal",      "url": "http://localhost:3001/mcp" },
    { "key": "filesystem",  "command": ["npx", "@modelcontextprotocol/server-filesystem", "/data"] }
  ]
}
```

- `url` → HTTP/SSE transport (`StreamableHttpMcpTransport`)
- `command` → Stdio transport — the JVM spawns the process (`StdioMcpTransport`)

MCP tools are discovered at agent startup; no code changes needed.

---

## Adding New Agents

**New leaf agent** — add to `config/pipeline-agent.json` + create `prompts/myagent.txt`:

```json
{
  "name": "Kabbalah",
  "description": "Questions about Jewish mysticism, Zohar, Sefirot, and Kabbalistic concepts",
  "type": "leaf",
  "promptFile": "prompts/kabbalah.txt",
  "tools": ["HEBREW_CALENDAR"],
  "retrieval": {
    "categories": ["Kabbalah", "Chasidut", "Jewish Thought"],
    "language": null,
    "topK": 20,
    "primaryOnly": false
  }
}
```

**New sub-router** — wrap leaves in a router node:

```json
{
  "name": "Kabbalah",
  "description": "Questions about Jewish mysticism",
  "type": "router",
  "children": [
    { "name": "Zohar",     "type": "leaf", "promptFile": "prompts/kabbalah_zohar.txt",    ... },
    { "name": "Sefirot",   "type": "leaf", "promptFile": "prompts/kabbalah_sefirot.txt",  ... }
  ]
}
```

**Override the model for one node** — add `"model"` at any level:

```json
{
  "name": "Kabbalah",
  "type": "leaf",
  "model": { "provider": "openai", "modelId": "gpt-4o" },
  ...
}
```

Model resolution: per-node override → parent's default → `agent.defaultModel`.

---

## Cost and Scale Estimates

### Embedding (Phase 3 — one-time cost)

| Scope | Approx. chunks | Estimated cost |
|---|---|---|
| P0 English only | ~50K | ~$0.30 |
| P0 bilingual | ~250K | ~$1.50 |
| Full corpus (no commentary) | ~500K | ~$3 |
| Full corpus + all commentary | ~2M | ~$12–40* |

\* Hebrew nikkud text tokenizes at ~2:1 token:character ratio vs ~1:4 for English, making it 8× more expensive per character. The P3 commentary corpus is overwhelmingly Hebrew.

### Qdrant storage

| Scope | Vectors | RAM (fp32) | RAM (INT8 quantization) |
|---|---|---|---|
| P0 bilingual | ~250K | ~1.5 GB | ~0.4 GB |
| Full corpus | ~2M | ~10 GB | ~2.5 GB |

### Agent inference (per question)

| Call | Tokens (approx) | Cost (approx) |
|---|---|---|
| Router classification (Haiku) | 200 in / 10 out | ~$0.0001 |
| Leaf call with RAG (Sonnet) | 3,000 in / 500 out | ~$0.018 |
| Query embedding (OpenAI) | 50 in | ~$0.000001 |

**Typical per-question cost: ~$0.02.**
