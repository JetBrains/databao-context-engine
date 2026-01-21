from databao_context_engine import DatabaoContextEngine


def run_all_results_tool(databao_context_engine: DatabaoContextEngine, run_name: str) -> str:
    return databao_context_engine.get_all_contexts_formatted(run_name=run_name)
