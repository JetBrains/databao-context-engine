from databao_context_engine.databao_engine import DatabaoContextEngine


def run_all_results_tool(databao_context_engine: DatabaoContextEngine) -> str:
    return databao_context_engine.get_all_contexts_formatted()
