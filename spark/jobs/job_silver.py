from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
import logging
import datetime
import json
import decase.utils as DE


def check_database_table_exists(spark_session: SparkSession, str_store_bucket_name: str, str_silver_table_name: str):
    """
    Check if the table exists in the database and create it if it does not exist.
    Parameters:
    - spark_session (SparkSession): The Spark session.
    - str_store_bucket_name (str): The name of the store bucket.
    - str_silver_table_name (str): The name of the silver table.
    """
    
    spark_session.sql(f'CREATE NAMESPACE IF NOT EXISTS nessie.silver')

    spark_session.sql(f'DROP TABLE IF EXISTS nessie.silver.{str_silver_table_name}')
    spark_session.sql(
        f'CREATE TABLE IF NOT EXISTS nessie.silver.{str_silver_table_name} (' + """
            id STRING COMMENT '{"order_sort": 0, "description": "Unique identifier for the brewery."}',
            name STRING COMMENT '{"order_sort": 1, "description": "Name of the brewery."}',
            brewery_type STRING COMMENT '{"order_sort": 2, "description": "Type of brewery.", "partition": {"enabled": true, "order_sort": 0}}',
            address_1 STRING COMMENT '{"order_sort": 3, "description": "First line of the brewery address."}',
            address_2 STRING COMMENT '{"order_sort": 4, "description": "Second line of the brewery address."}',
            address_3 STRING COMMENT '{"order_sort": 5, "description": "Third line of the brewery address."}',
            city STRING COMMENT '{"order_sort": 6, "description": "City where the brewery is located.", "partition": {"enabled": true, "order_sort": 3}}',
            state_province STRING COMMENT '{"order_sort": 7, "description": "State or province where the brewery is located.", "partition": {"enabled": true, "order_sort": 2}}',
            postal_code STRING COMMENT '{"order_sort": 8, "description": "Postal code of the brewery."}',
            country STRING COMMENT '{"order_sort": 9, "description": "Country where the brewery is located.", "partition": {"enabled": true, "order_sort": 1}}',
            longitude FLOAT COMMENT '{"order_sort": 10, "description": "Longitude of the brewery location."}',
            latitude FLOAT COMMENT '{"order_sort": 11, "description": "Latitude of the brewery location."}',
            phone BIGINT COMMENT '{"order_sort": 12, "description": "Phone number of the brewery."}',
            website_url STRING COMMENT '{"order_sort": 13, "description": "URL of the brewery website."}',
            state STRING COMMENT '{"order_sort": 14, "description": "State of the brewery."}',
            street STRING COMMENT '{"order_sort": 15, "description": "Street of the brewery."}'
        )
        USING iceberg
        PARTITIONED BY (
            brewery_type,
            country,
            state_province,
            city
        )
    """ + f" LOCATION 's3a://{str_store_bucket_name}/warehouse/silver/{str_silver_table_name}/' ")

def get_columns_partitioned_by_schema(list_schema: list) -> list:
    """
    Get the columns that are partitioned according to the schema.
    Parameters:
    - list_schema (list): List of dictionaries with the schema of the table.
    Returns:
    - list: List of columns that are partitioned.
    """

    lst_return = []
    for dcol in list_schema:
        if dcol['comment'].get('partition',{}).get('enabled', False):
            lst_return.append(dcol)

    lst_return.sort(key=lambda x: x['comment']['partition']['order_sort'])
    return [col['col_name'] for col in lst_return]


def adjust_columns_partition(df: DataFrame, lst_partition: list) -> DataFrame:
    """	
    Adjusts the columns partitioned by schema.
    Parameters:
    - df (DataFrame): The input Spark DataFrame.
    - lst_partition (list): List of columns that are partitioned.
    Returns:
    - DataFrame: The transformed DataFrame with the partitioned columns adjusted.
    """

    for col in lst_partition:
        df = df.withColumn(col, F.upper(F.regexp_replace(F.trim(F.col(col)), r'\s+', '_')))
    return df

def convert_comment_from_json(list_schema: list) -> list:
    """	
    Converts the comment field from JSON to dictionary.
    Parameters:
    - list_schema (list): List of dictionaries with the schema of the table.
    Returns:
    - list: List of dictionaries with the schema of the table with the comment field converted to dictionary.
    """

    for dcol in list_schema:
        if 'comment' in dcol and DE.is_valid_json(dcol['comment']):
            dcol['comment'] = json.loads(dcol['comment'])
        else:
            dcol['comment'] = {
                'partition': {},
                'order_sort': 0,
            }
    return list_schema

def remove_additional_columns(list_schema: list) -> list:
    """
    Removes additional columns from the schema.
    Parameters:
    - list_schema (list): List of dictionaries with the schema of the table.
    Returns:
    - list: List of dictionaries with the schema of the table without additional columns
    """

    return [dcol for dcol in list_schema if '#' not in dcol['col_name']]

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    logger.info('Running job_silver_app...')
    spark_session = DE.get_spark_session('job_silver_app')
    str_datetime_ref = spark_session.conf.get("spark.job_silver_app.datetime_ref", '1900-01-01 00:00:00')
    str_bucket_name = spark_session.conf.get("spark.job_silver_app.bucket_name", 'undefined')
    str_dataset_name = spark_session.conf.get("spark.job_silver_app.dataset_name", 'undefined')
    str_store_bucket_name = spark_session.conf.get("spark.job_silver_app.store_bucket_name", 'undefined')
    str_silver_table_name = spark_session.conf.get("spark.job_silver_app.silver_table_name", 'undefined')
    logger.info(f'Str datetime reference: {str_datetime_ref}, bucket name: {str_bucket_name}, dataset name: {str_dataset_name}, store bucket name: {str_store_bucket_name}, silver table name: {str_silver_table_name}')
    datetime_ref = datetime.datetime.strptime(str_datetime_ref, '%Y-%m-%d_%H:%M:%S')
    str_prefix_bucket = f"s3a://{str_bucket_name}/{str_dataset_name}/sys_file_date={datetime_ref.strftime('%Y-%m-%d')}/"
    logger.info(f'Str prefix bucket: {str_prefix_bucket}')

    lst_prefix_bucket = str_prefix_bucket.split('/')
    str_bucket_name = lst_prefix_bucket[2]
    str_prefix_path = '/'.join(lst_prefix_bucket[3:])
    logger.info(f'Bucket name: {str_bucket_name}, prefix path: {str_prefix_path}')

    minio_endpoint = spark_session.conf.get("spark.hadoop.fs.s3a.endpoint", 'undefined')
    minio_access_key = spark_session.conf.get("spark.hadoop.fs.s3a.access.key", 'undefined')
    minio_secret_key = spark_session.conf.get("spark.hadoop.fs.s3a.secret.key", 'undefined')
    logger.info(f'MinIO endpoint: {minio_endpoint}, access key: {minio_access_key}')

    if DE.check_minio_prefix_exists(minio_endpoint, minio_access_key, minio_secret_key, str_bucket_name, str_prefix_path, logger):
        logger.info(f"Path {str_prefix_bucket} exists.")
        df = (
            spark_session.read
                .option('inferSchema', 'true')
                .json(str_prefix_bucket)
        )
        df.printSchema()

        int_qtd_lines = df.count()
        logger.info(f"Number of lines: {int_qtd_lines}")
        if int_qtd_lines > 0:

            check_database_table_exists(spark_session, str_store_bucket_name, str_silver_table_name)

            describe_result = spark_session.sql("DESCRIBE TABLE nessie.silver.tab_brewery").distinct().collect()
            describe_list = [row.asDict() for row in describe_result]
            describe_list = remove_additional_columns(describe_list)
            describe_list = convert_comment_from_json(describe_list)
          
            # Log the result
            logger.info(f"Result schema columns: {describe_list}")

            df = DE.cast_columns_types_by_schema(df, describe_list, logger)

            lst_partition = get_columns_partitioned_by_schema(describe_list)
            logger.info(f"Sort column name partition: {lst_partition}")

            # For columns partition TRIM, UPPER AND REPLACE SPACES BETWEEN WORDS
            df = adjust_columns_partition(df, lst_partition)

            describe_list.sort(key=lambda x: x['comment']['order_sort'])
            lst_sorted_columns = [col['col_name'] for col in describe_list]
            logger.info(f"Sorted columns: {lst_sorted_columns}")
            df = df.select(*lst_sorted_columns)

            # Mode write without delta table
            #str_output_path = f"s3a://{str_store_bucket_name}/warehouse/dbw/tab_brewery/"
            #logger.info(f"Output path: {str_output_path}")
            #df.write.partitionBy(*lst_partition).mode("overwrite").parquet(str_output_path)
            #logger.info(f"Data written to {str_output_path} by partitions {lst_partition}.")

            df.write.mode("overwrite").saveAsTable(f"nessie.silver.{str_silver_table_name}")

        else:
            logger.info("No data to process.")
    else:
        logger.info(f"Path {str_prefix_bucket} does not exist.")

    spark_session.stop()
    logger.info('Job finished.')
