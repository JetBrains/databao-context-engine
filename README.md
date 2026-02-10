[![official project](https://jb.gg/badges/official.svg)](https://github.com/JetBrains#jetbrains-on-github)
[![PyPI version](https://img.shields.io/pypi/v/databao-context-engine.svg)](https://pypi.org/project/databao-context-engine)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://github.com/JetBrains/databao-context-engine?tab=License-1-ov-file)

[//]: # ([![Python versions]&#40;https://img.shields.io/pypi/pyversions/databao-context-engine.svg&#41;]&#40;https://pypi.org/project/databao-context-engine/&#41;)


<h1 align="center">Databao Context Engine</h1>
<p align="center">
 <b>Semantic context for your LLMs — generated automatically.</b><br/>
 No more copying schemas. No manual documentation. Just accurate answers.
</p>
<p align="center"> 
  <a href="https://databao.app">Website</a> •
  <a href="#quickstart">Quickstart</a> •
  <a href="#supported-data-sources">Data Sources</a> •
  <a href="#contributing">Contributing</a>
</p>

---

## What is Databao Context Engine?

Databao Context Engine is a CLI tool that **automatically generates governed semantic context** from your databases, BI tools, documents, and spreadsheets.

Integrate it with any LLM to deliver **accurate, context-aware answers** — without copying schemas or writing documentation by hand.

```
Your data sources → Context Engine → Unified semantic graph → Any LLM
```

## Why choose Databao Context Engine?

| Feature                    | What it means for you                                          |
|----------------------------|----------------------------------------------------------------|
| **Auto-generated context** | Extracts schemas, relationships, and semantics automatically   |
| **Runs locally**           | Your data never leaves your environment                        |
| **MCP integration**        | Works with Claude Desktop, Cursor, and any MCP-compatible tool |
| **Multiple sources**       | Databases, dbt projects, spreadsheets, documents               |
| **Built-in benchmarks**    | Measure and improve context quality over time                  |
| **LLM agnostic**           | OpenAI, Anthropic, Ollama, Gemini — use any model              |
| **Governed & versioned**   | Track, version, and share context across your team             |
| **Dynamic or static**      | Serve context via MCP server or export as artifact             |

## Installation

Databao Context Engine is [available on PyPI](https://pypi.org/project/databao-context-engine/) and can be installed with uv, pip, or another package manage.

### Using uv

1. Install Databao Context Engine:

   ```bash
   uv tool install databao-context-engine
   ```

1. Add it to your PATH:

   ```bash
   uv tool update-shell
   ```

1. Verify the installation:

   ```bash
   dce --help
   ```

### Using pip

1. Install Databao Context Engine:

   ```bash
   pip install databao-context-engine
   ```

1. Verify the installation:

   ```bash
   dce --help
   ```

##  Supported data sources

* <img src="https://cdn.simpleicons.org/postgresql/316192" width="16" height="16" alt=""> PostgreSQL
* <img src="https://cdn.simpleicons.org/mysql/4479A1" width="16" height="16" alt=""> MySQL
* <img src="https://cdn.simpleicons.org/sqlite/003B57" width="16" height="16" alt=""> SQLite
* <img src="https://cdn.simpleicons.org/duckdb/FFF000" width="16" height="16" alt=""> DuckDB
* <img src="https://cdn.simpleicons.org/dbt/FF694B" width="16" height="16" alt=""> dbt projects
* 📄 Documents & spreadsheets *(coming soon)*

##  Supported LLMs

| Provider      | Configuration                                |
|---------------|----------------------------------------------|
| **Ollama**    | `languageModel: OLLAMA`: runs locally, free  |
| **OpenAI**    | `languageModel: OPENAI`: requires an API key |
| **Anthropic** | `languageModel: CLAUDE`: requires an API key |
| **Google**    | `languageModel: GEMINI`: requires an API key |

## Quickstart

### 1. Create a project

1. Create a new directory for your project and navigate to it:

   ```bash
   mkdir dce-project && cd dce-project
   ```

1. Initialize a new project:

   ```bash
   dce init
   ```

### 2. Configure data sources

1. When prompted, agree to create a new datasource.
   You can also use the `dce datasource add` command.

1. Provide the data source type and its name.

1. Open the config file that was created for you in your editor and fill in the connection details.

1. Repeat these steps for all data sources you want to include in your project.

1. If you have data in Markdown or text files,
   you can add them to the `dce/src/files` directory.

### 3. Build context

1. To build the context, run the following command:

   ```bash
   dce build
   ```

### 4. Use Context with Your LLM

**Option A: Dynamic via MCP Server**

Databao Context Engine exposes the context through a local MCP Server, so your agent can access the latest context at runtime.

1. In **Claude Desktop**, **Cursor**, or another MCP-compatible agent, add the following configuration.
   Replace `dce-project/` with the path to your project directory:
  
   ```json 
   # claude_desktop_config.json, mcp.json, or similar
   
   {
     "mcpServers": {
       "dce": {
         "command": "dce mcp",
         "args": ["--project-dir", "dce-project/"]
       }
     }
   }
   ```

1. Save the file and restart your agent.

1. Open a new chat, in the chat window, select the `dce` server, and ask questions related to your project context.

**Option B: Static artifact**

Even if you don’t have Claude or Cursor installed on your local machine,
you can still use the context built by Databao Context Engine by pasting it directly into your chat with an AI assistant.

1. Navigate to `dce-project/output/` and open the directory with the latest run.

1. Attach the `all_results.yaml` file to your chat with the AI assistant or copy and paste its contents into your chat.

##  Contributing

We’d love your help! Here’s how to get involved:

- ⭐ **Star this repo** — it helps others find us!
- 🐛 **Found a bug?** [Open an issue](https://github.com/JetBrains/databao-context-engine/issues)
- 💡 **Have an idea?** We’re all ears — create a feature request
- 👍 **Upvote issues** you care about — helps us prioritize
- 🔧 **Submit a PR**
- 📝 **Improve docs** — typos, examples, tutorials — everything helps!

New to open source? No worries! We're friendly and happy to help you get started. 🌱

For more details, see [CONTRIBUTING](CONTRIBUTING.md).

## 📄 License

Apache 2.0 — use it however you want. See the [LICENSE](LICENSE.md) file for details.

---

<p align="center">
 <b>Like Databao Context Engine?</b> Give us a ⭐ — it means a lot!
</p>

<p align="center">
 <a href="https://databao.app">Website</a> •
 <a href="https://discord.gg/hEUqCcWdVh">Discord</a>
</p>
