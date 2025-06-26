from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from airflow.utils.trigger_rule import TriggerRule
from typing import Any
import os

MINIO_GOLDEN_BUCKET_NAME = 'datalake-gold' # Variable.get("MINIO_GOLDEN_BUCKET_NAME")
MINIO_SILVER_BUCKET_NAME = 'datalake-silver'
MINIO_ENDPOINT = 'minio:9000' # Variable.get("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = 'admin' # Variable.get("minio_access_key")
MINIO_SECRET_KEY = 'password' # Variable.get("minio_secret_key")
MINIO_DATALAKE_WAREHOUSE = 's3a://datalake-gold/warehouse' # Variable.get("MINIO_DATALAKE_WAREHOUSE")
NESSIE_URI = 'http://nessie:19120/api/v1' # Variable.get("NESSIE_URI")
NESSIE_SILVER_TABLE_NAME = 'tab_brewery' # Variable.get("NESSIE_SILVER_TABLE_NAME")
NESSIE_GOLDEN_TABLE_NAME = 'tab_brewery_summary' # Variable.get("NESSIE_GOLDEN_TABLE_NAME")

# Access the host path from the environment variable
HOST_SPARK_DIR = f"{os.getenv('CURRENT_DIR', '/default/path/if/not/set')}/spark"



def create_notification_message(
    execution_date: str, 
    dag_id: str, 
    message: str, 
    task_init_seq: Any, 
    task_end_seq: Any
):
    """
    Create a formatted notification message detailing DAG execution.

    Args:
        execution_date (str): The execution date of the DAG.
        dag_id (str): The identifier of the DAG.
        message (str): Custom message to include in the notification.
        task_init_seq (Any): Task instance containing the start_date attribute.
        task_end_seq (Any): Task instance containing the end_date attribute.

    Returns:
        str: A formatted notification message.
    """
    duration_str = "00:00:00"  # Default duration

    try:
        start_date: datetime = task_init_seq.start_date
        end_date: datetime = task_end_seq.end_date
        if start_date and end_date:
            duration: timedelta = end_date - start_date
            # Format duration to HH:MM:SS
            total_seconds = int(duration.total_seconds())
            duration_obj = timedelta(seconds=total_seconds)
            duration_str = str(duration_obj)
    except AttributeError as e:
        # Log the exception if logging is set up
        # For example: logger.error(f"Attribute error: {e}")
        pass  # Retain default duration_str

    notification_message = (
        f"*Dag*: {dag_id}\n"
        f"*Parameter Date*: {execution_date}\n"
        f"*Total Time*: {duration_str}\n"
        f"*Msg*: {message}"
    )
    
    #send by slack teams or email
    print(notification_message)

def create_failure_notification(
    task_id: str, 
    dag_id: str, 
    logical_time: str, 
    execution_time: str, 
    log_url: str
) -> str:
    """
    Create a formatted Slack message for task failure notifications.

    Args:
        task_id (str): The identifier of the failed task.
        dag_id (str): The identifier of the DAG.
        logical_time (str): The logical execution date of the DAG.
        execution_time (str): The actual execution date/time.
        log_url (str): URL to the task's log for debugging.

    Returns:
        str: A formatted Slack message indicating task failure.
    """
    slack_message = (
        ":x: *Task Failed*\n"
        f"*Task*: {task_id}\n"
        f"*Dag*: {dag_id}\n"
        f"*Parameter Date*: {logical_time}\n"
        f"*Execution Date*: {execution_time}\n"
        f"<{log_url}|*View Logs*>"
    )
    
    return slack_message



def event_failure_tasks(context):
    """
    Send a Slack notification when a task fails.

    Args:
        context (dict): Context dictionary with task instance information.
    """
    task_instance = context.get("task_instance")
    task_id = task_instance.task_id
    dag_id = task_instance.dag_id
    logical_time = context.get("execution_date")
    execution_time = task_instance.execution_date
    log_url = task_instance.log_url
    slack_message = create_failure_notification(
        task_id, dag_id, logical_time, execution_time, log_url
    )
    # Send the Slack message
    # slack_hook.send(slack_message)
    print(slack_message)

def send_notification_message(msg):
    print(msg)
    """
    Send a notification message.

    Args:
        msg (str): The message to send.
    """
    print(msg)

def get_datetime_UTC_SaoPaulo(execution_date: datetime) -> str:
    return (execution_date - timedelta(hours=3)).strftime('%Y-%m-%d_%H:%M:%S')

default_args = {
    'owner': 'herculanocm',
    "email": ["herculanocm@outlook.com"],
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'email_on_success': False,
    'on_failure_callback': event_failure_tasks,
    'on_success_callback': None,
    'retries': 1,
    'retry_delay': timedelta(seconds=2),
}

with DAG(
        dag_id='3_datalake_gold_spark_dag',
        schedule_interval=None,
        start_date=datetime(2024, 10, 18),
        default_args=default_args,
        params={"custom_param": "default_value"},
        catchup=False,
        tags=['datalake', 'pipe', 'gold'],
        user_defined_macros={
            'get_datetime_UTC_SaoPaulo': get_datetime_UTC_SaoPaulo,
            'create_notification_message': create_notification_message,
        }
) as dag:
    
    task_init_seq_01 = EmptyOperator(
        task_id='task_init_seq_01'
    )

    

    task_run_spark_job_gold = DockerOperator(
        task_id='task_run_spark_job_gold',
        image='herculanocm/spark:3.4.1',
        api_version='auto',
        auto_remove=True,
        command="""
        bash -c "pip install /app/data/python_libs/decase/dist/decase-0.0.1-py3-none-any.whl && \
        /opt/bitnami/spark/bin/spark-submit \
            --master local[*] \
            --deploy-mode client \
            --conf spark.sql.catalog.nessie=org.apache.iceberg.spark.SparkCatalog \
            --conf spark.sql.catalog.nessie.catalog-impl=org.apache.iceberg.nessie.NessieCatalog \
            --conf spark.sql.catalog.nessie.warehouse={{ params.minio_datalake_warehouse }} \
            --conf spark.sql.catalog.nessie.uri={{ params.nessie_uri }} \
            --conf spark.sql.catalog.nessie.ref=main \
            --conf spark.sql.catalog.nessie.auth-type=NONE \
            --conf spark.hadoop.fs.s3a.endpoint=http://{{ params.minio_endpoint }} \
            --conf spark.hadoop.fs.s3a.access.key={{ params.minio_access_key }} \
            --conf spark.hadoop.fs.s3a.secret.key={{ params.minio_secret_key }} \
            --conf spark.hadoop.fs.s3a.path.style.access=true \
            --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
            --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
            --conf spark.job_gold_app.table_name={{ params.table_name }} \
            --conf spark.job_gold_app.datetime_ref={{ get_datetime_UTC_SaoPaulo(execution_date) }} \
            --conf spark.job_gold_app.store_bucket_name={{ params.minio_golden_bucket_name }} \
            --conf spark.job_gold_app.golden_table_name={{ params.table_name_summary }} \
            --conf spark.job_gold_app.minio_silver_bucket_name={{ params.minio_silver_bucket_name }} \
            /app/data/jobs/job_gold.py"
        """,
        docker_url='unix://var/run/docker.sock',
        network_mode='my-net',
        mounts=[
            Mount(
                source=HOST_SPARK_DIR,
                target='/app/data',
                type='bind'
            ),
        ],
        environment={
            'SPARK_HOME': '/opt/bitnami/spark',
        },
        mount_tmp_dir=False,
        params={
            'minio_endpoint': MINIO_ENDPOINT,
            'minio_access_key': MINIO_ACCESS_KEY,
            'minio_secret_key': MINIO_SECRET_KEY,
            'minio_datalake_warehouse': MINIO_DATALAKE_WAREHOUSE,
            'nessie_uri': NESSIE_URI,
            'table_name': NESSIE_SILVER_TABLE_NAME,
            'table_name_summary': NESSIE_GOLDEN_TABLE_NAME,
            'minio_golden_bucket_name': MINIO_GOLDEN_BUCKET_NAME,
            'minio_silver_bucket_name': MINIO_SILVER_BUCKET_NAME,
        },
    )

    task_end_seq_01 = EmptyOperator(
        task_id='task_end_seq_01',
        trigger_rule=TriggerRule.NONE_FAILED
    )

    task_calc_total_time = PythonOperator(
        task_id='task_calc_total_time',
        python_callable=send_notification_message,
        op_args={'msg': """{{ create_notification_message(execution_date, dag.dag_id, 'Finished', dag_run.get_task_instance('task_init_seq_01'), dag_run.get_task_instance('task_end_seq_01')) }}"""},
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

task_init_seq_01 >> task_run_spark_job_gold >> task_end_seq_01 >> task_calc_total_time