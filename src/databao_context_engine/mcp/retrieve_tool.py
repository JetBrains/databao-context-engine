import datetime

from databao_context_engine import DatabaoContextEngine


def run_retrieve_tool(*, databao_context_engine: DatabaoContextEngine, text: str, limit: int | None = None) -> str:
    """Execute the retrieve flow for MCP and return the matching display texts."""
    retrieve_results = databao_context_engine.search_context(retrieve_text=text, limit=limit, export_to_file=False)

    display_results = [context_search_result.context_result for context_search_result in retrieve_results]

    display_results.append(f"\nToday's date is {datetime.date.today()}")

    return "\n".join(display_results)
