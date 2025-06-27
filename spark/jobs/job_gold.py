from pyspark.sql import SparkSession
import logging
import datetime
import decase.utils as DE
import time

def ensure_schema_table_exists(spark_session, table_name):
    spark_session.sql("CREATE SCHEMA IF NOT EXISTS gold.dw")

    spark_session.sql(f"""
    CREATE TABLE IF NOT EXISTS gold.dw.{table_name} (
        sys_file_date DATE,
        brewery_type STRING,
        country STRING,
        qtd BIGINT
    )
    USING ICEBERG
    PARTITIONED BY (sys_file_date)
    """)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    logger.info('Running job_gold_app...')
    
    spark_session = (
    SparkSession.builder
    .appName("job_golden_app")
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
    .config("spark.sql.catalog.spark_catalog.type", "hive")
    .getOrCreate()
    )

    str_datetime_ref = spark_session.conf.get("spark.job_gold_app.datetime_ref", '1900-01-01 00:00:00')
    str_table_name = spark_session.conf.get("spark.job_gold_app.table_name", 'undefined')
    str_golden_table_name = spark_session.conf.get("spark.job_gold_app.golden_table_name", 'undefined')
    str_silver_bucket_name = spark_session.conf.get("spark.job_gold_app.minio_silver_bucket_name", 'undefined')
    str_store_bucket_name = spark_session.conf.get("spark.job_gold_app.store_bucket_name", 'undefined')
    logger.info(f'Str datetime reference: {str_datetime_ref}, source str_table_name: {str_table_name}, str_golden_table_name: {str_golden_table_name}, store bucket name: {str_store_bucket_name}')
    datetime_ref = datetime.datetime.strptime(str_datetime_ref, '%Y-%m-%d_%H:%M:%S')

    minio_endpoint = spark_session.conf.get("spark.hadoop.fs.s3a.endpoint", 'undefined')
    minio_access_key = spark_session.conf.get("spark.hadoop.fs.s3a.access.key", 'undefined')
    minio_secret_key = spark_session.conf.get("spark.hadoop.fs.s3a.secret.key", 'undefined')
    logger.info(f'MinIO endpoint: {minio_endpoint}, access key: {minio_access_key}')

    # Setting spark config spark.sql.catalog.local.warehouse
    str_silver_warehouse = f"s3a://{str_silver_bucket_name}/warehouse"
    logger.info(f'Str silver warehouse: {str_silver_warehouse}')
    spark_session.conf.set("spark.sql.catalog.silver", "org.apache.iceberg.spark.SparkCatalog")
    spark_session.conf.set("spark.sql.catalog.silver.type", "hadoop")
    spark_session.conf.set("spark.sql.catalog.silver.warehouse", str_silver_warehouse)

    str_golden_warehouse = f"s3a://{str_store_bucket_name}/warehouse"
    logger.info(f'Str golden warehouse: {str_golden_warehouse}')
    spark_session.conf.set("spark.sql.catalog.gold", "org.apache.iceberg.spark.SparkCatalog")
    spark_session.conf.set("spark.sql.catalog.gold.type", "hadoop")
    spark_session.conf.set("spark.sql.catalog.gold.warehouse", str_golden_warehouse)

    return_obj = DE.get_qtd_and_size_minio(
        minio_endpoint,
        minio_access_key,
        minio_secret_key,
        str_silver_bucket_name,
        '/warehouse/dw/tab_brewery/data',
        logger
    )
    logger.info(f"Total objects: {return_obj['total_objects']}, Total bytes: {return_obj['total_bytes']}")
    
    
    str_query_silver = f"""

        select 
            sys_file_date,
            brewery_type,
            country,
            count(*) as qtd
        from silver.dw.{str_table_name}
        where sys_file_date = '{datetime_ref.strftime('%Y-%m-%d')}'
        group by 1,2,3
        order by 1,2,3
                                   
    """

    logger.info(f'Executing query: {str_query_silver}')

    start_time = time.time()
    df_brewery = spark_session.sql(str_query_silver)
    

    df_brewery.printSchema()
    df_brewery.show()

    logger.info('Ensuring schema table exists...')
    ensure_schema_table_exists(spark_session, str_golden_table_name)

    logger.info('Writing data to golden table...')
    df_brewery.write.format("iceberg") \
                .mode("overwrite") \
                .option("write.metadata.delete-after-commit.enabled", "true") \
                .option("write.metadata.previous-versions-max", "10") \
                .saveAsTable(f"gold.dw.{str_golden_table_name}")

    elapsed_time = time.time() - start_time
    logger.info(f"Geração de agregações e gravação gold: {elapsed_time:.2f} segundos - Total de linhas: {df_brewery.count()}")

    logger.info('Job finished.')