from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import logging
import datetime
import decase.utils as DE
import time

def remove_extra_columns(df, describe_list):
    """
    Remove do DataFrame as colunas que não estão presentes no describe_list.
    Parâmetros:
    - df: DataFrame do PySpark.
    - describe_list: Lista de dicionários com as colunas válidas.
    Retorna:
    - DataFrame apenas com as colunas do describe_list.
    """
    valid_cols = [col['col_name'] for col in describe_list]
    cols_to_keep = [col for col in df.columns if col in valid_cols]
    return df.select(*cols_to_keep)

def sort_columns_by_order(df, describe_list):
    """
    Ordena as colunas do DataFrame conforme o campo 'order' do describe_list.
    Parâmetros:
    - df: DataFrame do PySpark.
    - describe_list: Lista de dicionários com o campo 'order'.
    Retorna:
    - DataFrame com as colunas ordenadas.
    """
    ordered_cols = [col['col_name'] for col in sorted(describe_list, key=lambda x: x['order'])]
    return df.select(*ordered_cols)

describe_list = [
    {'col_name': 'id', 'data_type': 'string', 'order': 1},
    {'col_name': 'name', 'data_type': 'string', 'order': 2},
    {'col_name': 'brewery_type', 'data_type': 'string', 'order': 3},
    {'col_name': 'address_1', 'data_type': 'string', 'order': 4},
    {'col_name': 'address_2', 'data_type': 'string', 'order': 5},
    {'col_name': 'address_3', 'data_type': 'string', 'order': 6},
    {'col_name': 'city', 'data_type': 'string', 'order': 7},
    {'col_name': 'state_province', 'data_type': 'string', 'order': 8},
    {'col_name': 'postal_code', 'data_type': 'string', 'order': 9},
    {'col_name': 'country', 'data_type': 'string', 'order': 10},
    {'col_name': 'longitude', 'data_type': 'float', 'order': 11},
    {'col_name': 'latitude', 'data_type': 'float', 'order': 12},
    {'col_name': 'phone', 'data_type': 'bigint', 'order': 13},
    {'col_name': 'website_url', 'data_type': 'string', 'order': 14},
    {'col_name': 'state', 'data_type': 'string', 'order': 15},
    {'col_name': 'street', 'data_type': 'string', 'order': 16},
]

def remove_additional_columns(list_schema: list) -> list:
    """
    Removes additional columns from the schema.
    Parameters:
    - list_schema (list): List of dictionaries with the schema of the table.
    Returns:
    - list: List of dictionaries with the schema of the table without additional columns
    """

    return [dcol for dcol in list_schema if '#' not in dcol['col_name']]

def ensure_schema_table_exists(spark_session):
    spark_session.sql("CREATE SCHEMA IF NOT EXISTS dw")

    spark_session.sql("""
    CREATE TABLE IF NOT EXISTS silver.dw.tab_brewery (
        id STRING,
        name STRING,
        brewery_type STRING,
        address_1 STRING,
        address_2 STRING,
        address_3 STRING,
        city STRING,
        state_province STRING,
        postal_code STRING,
        country STRING,
        longitude FLOAT,
        latitude FLOAT,
        phone BIGINT,
        website_url STRING,
        state STRING,
        street STRING,
        sys_file_date DATE
    )
    USING ICEBERG
    PARTITIONED BY (sys_file_date)
    """)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    logger.info('Running job_silver_app...')


    spark_session = (
    SparkSession.builder
    .appName("job_silver_app")
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
    .config("spark.sql.catalog.spark_catalog.type", "hive")
    .config("spark.sql.catalog.silver", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.silver.type", "hadoop")
    .getOrCreate()
    )

    str_datetime_ref = spark_session.conf.get("spark.job_silver_app.datetime_ref", '1900-01-01 00:00:00')
    str_bucket_name = spark_session.conf.get("spark.job_silver_app.bucket_name", 'undefined')
    str_dataset_name = spark_session.conf.get("spark.job_silver_app.dataset_name", 'undefined')
    str_store_bucket_name = spark_session.conf.get("spark.job_silver_app.store_bucket_name", 'undefined')
    str_silver_table_name = spark_session.conf.get("spark.job_silver_app.silver_table_name", 'undefined')
    logger.info(f'Str datetime reference: {str_datetime_ref}, bucket name: {str_bucket_name}, dataset name: {str_dataset_name}, store bucket name: {str_store_bucket_name}, silver table name: {str_silver_table_name}')
    datetime_ref = datetime.datetime.strptime(str_datetime_ref, '%Y-%m-%d_%H:%M:%S')
    str_prefix_bucket = f"s3a://{str_bucket_name}/{str_dataset_name}/sys_file_date={datetime_ref.strftime('%Y-%m-%d')}/"
    logger.info(f'Str prefix bucket: {str_prefix_bucket}')

    # Setting spark config spark.sql.catalog.local.warehouse
    spark_session.conf.set("spark.sql.catalog.silver.warehouse", f"s3a://{str_store_bucket_name}/warehouse")

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

        return_obj = DE.get_qtd_and_size_minio(
            minio_endpoint,
            minio_access_key,
            minio_secret_key,
            str_bucket_name,
            str_prefix_path,
            logger
        )
        logger.info(f"Total objects: {return_obj['total_objects']}, Total bytes: {return_obj['total_bytes']}")

        start_time = time.time()
        df = (
            spark_session.read
                .option('inferSchema', 'true')
                .json(str_prefix_bucket)
        )
        elapsed_time = time.time() - start_time
        logger.info(f"Tempo para leitura dos arquivos: {elapsed_time:.2f} segundos - Total de linhas: {df.count()}")
        df.printSchema()

        int_qtd_lines = df.count()
        logger.info(f"Number of lines: {int_qtd_lines}")
        if int_qtd_lines > 0:
            logger.info("Processing data...")

            start_time = time.time()

            logger.info('Removing extra columns...')
            df = remove_extra_columns(df, describe_list)
            logger.info('Casting columns types by schema...')
            df = DE.cast_columns_types_by_schema(df, describe_list, logger)
            logger.info('Sorting columns by order...')
            df = sort_columns_by_order(df, describe_list)
            logger.info('Adding sys_file_date column...')
            df = df.withColumn("sys_file_date", F.lit(datetime_ref.strftime('%Y-%m-%d')))
            # Converting sys_file_date to date type
            df = df.withColumn("sys_file_date", F.to_date(F.col("sys_file_date"), "yyyy-MM-dd"))

            logger.info('Ensuring schema table exists...')
            ensure_schema_table_exists(spark_session)
            
            logger.info('Storing dataframe as iceberg table...')
            df.write.format("iceberg") \
                .mode("overwrite") \
                .option("write.metadata.delete-after-commit.enabled", "true") \
                .option("write.metadata.previous-versions-max", "10") \
                .saveAsTable(f"silver.dw.tab_brewery")
                
            elapsed_time = time.time() - start_time
            logger.info(f"Tempo para tratamento e gravação na silver: {elapsed_time:.2f} segundos")

        else:
            logger.info("No data to process.")
    else:
        logger.info(f"Path {str_prefix_bucket} does not exist.")

    spark_session.stop()
    logger.info('Job finished.')
