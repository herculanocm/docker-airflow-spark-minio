from pyspark.sql import SparkSession
import logging
import datetime
import decase.utils as DE

def check_database_table_exists(spark_session: SparkSession, str_store_bucket_name: str, str_golden_table_name: str):
    """
    Check if the table exists in the database and create it if it does not exist.
    Parameters:
    - spark_session (SparkSession): The Spark session.
    - str_store_bucket_name (str): The name of the store bucket.
    - str_golden_table_name (str): The name of the golden table.
    """
    
    spark_session.sql('CREATE NAMESPACE IF NOT EXISTS nessie.gold')

    spark_session.sql(f'DROP TABLE IF EXISTS nessie.gold.{str_golden_table_name}')
    spark_session.sql(
        f' CREATE TABLE IF NOT EXISTS nessie.gold.{str_golden_table_name} ( ' + """
            brewery_type STRING,
            country STRING,
            qtd BIGINT
        )
        USING iceberg
        PARTITIONED BY (
            brewery_type,
            country
        )
    """ + f" LOCATION 's3a://{str_store_bucket_name}/warehouse/gold/{str_golden_table_name}/' "
    )

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    logger.info('Running job_gold_app...')
    spark_session = DE.get_spark_session('job_gold_app')
    str_datetime_ref = spark_session.conf.get("spark.job_gold_app.datetime_ref", '1900-01-01 00:00:00')
    str_table_name = spark_session.conf.get("spark.job_gold_app.table_name", 'undefined')
    str_golden_table_name = spark_session.conf.get("spark.job_gold_app.golden_table_name", 'undefined')
    str_store_bucket_name = spark_session.conf.get("spark.job_gold_app.store_bucket_name", 'undefined')
    logger.info(f'Str datetime reference: {str_datetime_ref}, source str_table_name: {str_table_name}, str_golden_table_name: {str_golden_table_name}, store bucket name: {str_store_bucket_name}')
    datetime_ref = datetime.datetime.strptime(str_datetime_ref, '%Y-%m-%d_%H:%M:%S')

    df_brewery = spark_session.sql(F"""

        select 
            brewery_type,
            country,
            count(*) as qtd
        from nessie.silver.{str_table_name}
        group by 
        brewery_type,
        country
        order by 1 , 2 
                                   
    """)
    int_qtd_lines = df_brewery.count()
    logger.info(f"Number of lines: {int_qtd_lines}")
    if int_qtd_lines > 0:
        check_database_table_exists(spark_session, str_store_bucket_name, str_golden_table_name)
        df_brewery.write.mode("overwrite").saveAsTable(f"nessie.gold.{str_golden_table_name}")
    else:
        logger.info("No data to process.")

    spark_session.stop()
    logger.info('Job finished.')