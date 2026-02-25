from databao_context_engine.plugins.databases.base_db_plugin import BaseDatabasePlugin
from databao_context_engine.plugins.databases.bigquery.bigquery_introspector import BigQueryIntrospector
from databao_context_engine.plugins.databases.bigquery.config_file import BigQueryConfigFile


class BigQueryDbPlugin(BaseDatabasePlugin[BigQueryConfigFile]):
    id = "jetbrains/bigquery"
    name = "BigQuery DB Plugin"
    supported = {"bigquery"}
    config_file_type = BigQueryConfigFile

    def __init__(self):
        super().__init__(BigQueryIntrospector())
