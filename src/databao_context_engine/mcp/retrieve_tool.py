import datetime

from databao_context_engine import DatabaoContextEngine


def run_retrieve_tool(
    *, databao_context_engine: DatabaoContextEngine, run_name: str | None, text: str, limit: int | None = None
) -> str:
    """
    Execute the retrieve flow for MCP and return the matching display texts
    Adds the current date to the end
    """

    retrieve_results = databao_context_engine.search_context(
        retrieve_text=text, run_name=run_name, limit=limit, export_to_file=False
    )

    display_results = [context_search_result.context_result for context_search_result in retrieve_results]

    display_results.append(f"\nToday's date is {datetime.date.today()}")

    return "\n".join(display_results)
