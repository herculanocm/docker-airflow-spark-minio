# DAG and Operators
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.utils.trigger_rule import TriggerRule
from typing import Any
import boto3
import logging

MINIO_LAND_BUCKET_NAME = 'datalake-bronze' # Variable.get("MINIO_LAND_BUCKET_NAME")
MINIO_SILVER_BUCKET_NAME = 'datalake-silver' # Variable.get("MINIO_SILVER_BUCKET_NAME")
MINIO_ENDPOINT = 'http://minio:9000' # Variable.get("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = 'admin' # Variable.get("minio_access_key")
MINIO_SECRET_KEY = 'password' # Variable.get("minio_secret_key")


def get_qtd_and_size_minio(endpoint_url: str, access_key: str, secret_key: str, bucket_name: str, prefix: str) -> dict:
    return_obj = {
        'total_bytes': 0,
        'total_objects': 0
    }
    try:

        # Initialize S3 client with MinIO configurations
        s3 = boto3.client(
            's3',
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=boto3.session.Config(signature_version='s3v4'),
            verify=False  # Set to True if SSL is enabled
        )

        # List objects under the specified prefix
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

        if 'Contents' in response:
            for obj in response['Contents']:
                return_obj['total_objects'] += 1
                return_obj['total_bytes'] += obj.get('Size', 0)
        else:
            print(f"No objects found under prefix '{prefix}' in bucket '{bucket_name}'.")
    
    except Exception as e:
        print(f"Unexpected error while calculating total bytes: {e}")


    return return_obj


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
    
    return notification_message

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

def calc_total_time(**kwargs):
    """
    Calculate the total execution time of the DAG and print logging information.
    """
    dag_run = kwargs.get('dag_run')
    start_date = dag_run.get_task_instance('task_init_seq_01').start_date
    end_date = dag_run.get_task_instance('task_end_seq_01').end_date
    total_seconds = (end_date - start_date).total_seconds()
    total_hours, total_seconds = divmod(total_seconds, 3600)
    total_minutes, total_seconds = divmod(total_seconds, 60)
    total_time = f"{int(total_hours):02d}:{int(total_minutes):02d}:{int(total_seconds):02d}"

    print(f"Total execution time: {total_time}")

    return_obj = get_qtd_and_size_minio(
        MINIO_ENDPOINT,
        MINIO_ACCESS_KEY,
        MINIO_SECRET_KEY,
        'datalake-silver',
        'warehouse/dw/tab_brewery/data'
    )
    print(f"Total objects on silver zone tab_brewery: {return_obj['total_objects']}, Total bytes: {return_obj['total_bytes']}")

    return_obj_gold = get_qtd_and_size_minio(
        MINIO_ENDPOINT,
        MINIO_ACCESS_KEY,
        MINIO_SECRET_KEY,
        'datalake-gold',
        'warehouse/dw/tab_brewery_summary/data'
    )
    print(f"Total objects on gold zone tab_brewery_summary: {return_obj_gold['total_objects']}, Total bytes: {return_obj_gold['total_bytes']}")


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

def get_datetime_UTC_SaoPaulo(execution_date: datetime) -> str:
    return (execution_date - timedelta(hours=3)).strftime('%Y-%m-%d_%H:%M:%S')

with DAG(
        dag_id='0_datalake_pipeline_breweries',
        schedule_interval="20 0 * * *",
        start_date=datetime(2024, 10, 18),
        default_args=default_args,
        params={"custom_param": "default_value"},
        catchup=False,
        tags=['datalake', 'pipeline', 'breweries'],
        user_defined_macros={
            'get_datetime_UTC_SaoPaulo': get_datetime_UTC_SaoPaulo
        }
) as dag:
    
    task_init_seq_01 = EmptyOperator(
        task_id='task_init_seq_01'
    )

    task_trigger_datalake_bronze_fetch_raw_data_api_dag = TriggerDagRunOperator(
        task_id='1_task_trigger_datalake_bronze_fetch_raw_data_api_dag',
        trigger_dag_id='1_datalake_bronze_fetch_raw_data_api_dag',
        conf={"custom_param": "default_value"},
        reset_dag_run=True,
        wait_for_completion=True,
        poke_interval=30,
    )

    task_trigger_datalake_silver_spark_dag = TriggerDagRunOperator(
        task_id='2_task_trigger_datalake_silver_spark_dag',
        trigger_dag_id='2_datalake_silver_spark_dag',
        conf={"custom_param": "default_value"},
        reset_dag_run=True,
        wait_for_completion=True,
        poke_interval=30,
    )

    task_trigger_datalake_gold_spark_dag = TriggerDagRunOperator(
        task_id='3_task_trigger_datalake_gold_spark_dag',
        trigger_dag_id='3_datalake_gold_spark_dag',
        conf={"custom_param": "default_value"},
        reset_dag_run=True,
        wait_for_completion=True,
        poke_interval=30,
    )

    task_end_seq_01 = EmptyOperator(
        task_id='task_end_seq_01',
        trigger_rule=TriggerRule.NONE_FAILED
    )

    task_calc_total_time = PythonOperator(
        task_id='task_calc_total_time',
        provide_context=True,
        python_callable=calc_total_time,
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

task_init_seq_01 >> task_trigger_datalake_bronze_fetch_raw_data_api_dag >> task_trigger_datalake_silver_spark_dag >> task_trigger_datalake_gold_spark_dag >> task_end_seq_01 >> task_calc_total_time