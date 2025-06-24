from airflow.plugins_manager import AirflowPlugin
from operators.fetch_api_operator import FetchApiOperator

class FetchApiPlugin(AirflowPlugin):
    name = "FetchApiPlugin"
    operators = [FetchApiOperator]