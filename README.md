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

Databao Context Engine is a Python library that **automatically generates governed semantic context** from your databases, BI tools, documents, and spreadsheets.

Use it with any LLM to deliver **accurate, context-aware answers** — without copying schemas or writing documentation by hand.

You can add Databao Context Engine as a standard Python dependency in your code or via Databao CLI (coming soon).

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

Databao Context Engine is [available on PyPI](https://pypi.org/project/databao-context-engine/) 
and can be installed with uv, pip, or another package manager.

### Using uv

```bash
uv add databao-context-engine
```

### Using pip

```bash
pip install databao-context-engine
```

##  Supported data sources

* Athena
* BigQuery
* ClickHouse
* DuckDB
* MSSQL
* MySQL
* PostgreSQL
* Snowflake
* SQLite
* dbt projects
* PDF files
* Markdown and text files

##  Supported LLMs

| Provider      | Configuration                                |
|---------------|----------------------------------------------|
| **Ollama**    | `languageModel: OLLAMA`: runs locally, free  |

## Quickstart

### 1. Create a domain

```python
# Initialize the domain in a temporary directory
from databao_context_engine import init_dce_domain
from pathlib import Path
import tempfile

domain_manager = init_dce_domain(Path(tempfile.mkdtemp()))

# Or use an existing project
from databao_context_engine import DatabaoContextDomainManager

domain_manager = DatabaoContextDomainManager(domain_dir=Path("domain_dir"))
```

### 2. Configure data sources

```python
from databao_context_engine import (
    CheckDatasourceConnectionResult,
    DatasourceConnectionStatus,
    DatasourceId,
    DatasourceType,
)

# Create a new datasource
postgres_datasource_id = domain_manager.create_datasource_config(
    DatasourceType(full_type="postgres"),
    datasource_name="my_postgres_datasource",
    config_content={
        "connection": {"host": "localhost", "user": "dev", "password": "pass"}
    },
).datasource.id

# Check the connection to the datasource is valid
check_result: dict[DatasourceId, CheckDatasourceConnectionResult] = domain_manager.check_datasource_connection()

assert len(check_result) == 1
assert check_result[postgres_datasource_id].connection_status == DatasourceConnectionStatus.VALID
```

### 3. Build context

```python
build_result = domain_manager.build_context()

assert len(build_result) == 1
assert build_result[0].datasource_id == postgres_datasource_id
assert build_result[0].datasource_type == DatasourceType(full_type="postgres")
assert build_result[0].context_file_path.is_file()
```

### 4. Use the built contexts

#### Create a context engine

```python
# Switch to the engine if you're already using a domain_manager
context_engine = domain_manager.get_engine_for_domain()

# Or directly create a context engine from the path to your DCE domaint
from databao_context_engine import DatabaoContextEngine

context_engine = DatabaoContextEngine(domain_dir=Path("path/to/project"))
```

#### Get all built contexts

```python
# Switch to the engine to use the context built
all_built_contexts = context_engine.get_all_contexts()
assert len(all_built_contexts) == 1
assert all_built_contexts[0].datasource_id == postgres_datasource_id

print(all_built_contexts[0].context)
```

#### Search in built contexts

```python
# Run a vector similarity search
results = context_engine.search_context("my search query")

print(f"Found {len(results)} results for query")
print(
    "\n\n".join(
        [f"{str(result.datasource_id)}\n{result.context_result}" for result in results]
    )
)
```

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
