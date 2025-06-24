import logging
from pyspark.sql import SparkSession
import boto3
import json
from botocore.exceptions import NoCredentialsError, ClientError
from pyspark.sql.types import StringType, DoubleType, DateType, TimestampType, IntegerType, BooleanType, LongType
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

def get_spark_session(str_app_name: str) -> SparkSession:
    return SparkSession.builder.appName(str_app_name) \
        .getOrCreate()

def check_minio_prefix_exists(endpoint_url: str, access_key: str, secret_key: str, bucket_name: str, prefix: str, logger: logging.Logger = None) -> bool:
    """
    Checks if a given prefix exists in a MinIO bucket by verifying the presence of objects under the prefix.

    Parameters:
    - endpoint_url (str): The endpoint URL for MinIO (e.g., 'http://localhost:9000').
    - access_key (str): Access key for MinIO.
    - secret_key (str): Secret key for MinIO.
    - bucket_name (str): The bucket to check.
    - prefix (str): The prefix to check (e.g., 'warehouse/tab_brewery/').
    - logger (logging.Logger, optional): Logger for logging information and errors.

    Returns:
    - bool: True if the prefix exists (has at least one object), False otherwise.
    """
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
        
        # List objects under the specified prefix with a maximum of 1 key
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix, MaxKeys=1)
        
        exists = 'Contents' in response and len(response['Contents']) > 0
        
        if logger:
            if exists:
                logger.info(f"Prefix '{prefix}' exists in bucket '{bucket_name}'.")
            else:
                logger.info(f"Prefix '{prefix}' does not exist or is empty in bucket '{bucket_name}'.")
        
        return exists
    except NoCredentialsError:
        if logger:
            logger.error("MinIO credentials not provided or incorrect.")
        return False
    except ClientError as e:
        if logger:
            logger.error(f"Client error while accessing MinIO: {e}")
        return False
    except Exception as e:
        if logger:
            logger.error(f"Unexpected error while checking prefix: {e}")
        return False
    
def is_valid_json(value: str) -> bool:
    """
    Checks if string is a valid JSON.

    Parameters:
    - value (str): The string to check.

    Returns:
    - bool: True if the string is valid JSON, False otherwise.
    """
    try:
        json.loads(value)
        return True
    except (ValueError, TypeError):
        return False
    
def check_minio_prefix_exists(endpoint_url: str, access_key: str, secret_key: str, bucket_name: str, prefix: str, logger: logging.Logger = None) -> bool:
    """
    Checks if a given prefix exists in a MinIO bucket by verifying the presence of objects under the prefix.

    Parameters:
    - endpoint_url (str): The endpoint URL for MinIO (e.g., 'http://localhost:9000').
    - access_key (str): Access key for MinIO.
    - secret_key (str): Secret key for MinIO.
    - bucket_name (str): The bucket to check.
    - prefix (str): The prefix to check (e.g., 'warehouse/tab_brewery/').
    - logger (logging.Logger, optional): Logger for logging information and errors.

    Returns:
    - bool: True if the prefix exists (has at least one object), False otherwise.
    """
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
        
        # List objects under the specified prefix with a maximum of 1 key
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix, MaxKeys=1)
        
        exists = 'Contents' in response and len(response['Contents']) > 0
        
        if logger:
            if exists:
                logger.info(f"Prefix '{prefix}' exists in bucket '{bucket_name}'.")
            else:
                logger.info(f"Prefix '{prefix}' does not exist or is empty in bucket '{bucket_name}'.")
        
        return exists
    except NoCredentialsError:
        if logger:
            logger.error("MinIO credentials not provided or incorrect.")
        return False
    except ClientError as e:
        if logger:
            logger.error(f"Client error while accessing MinIO: {e}")
        return False
    except Exception as e:
        if logger:
            logger.error(f"Unexpected error while checking prefix: {e}")
        return False
    

def cast_columns_types_by_schema(df: DataFrame, list_schema: list, logger: logging.Logger) -> DataFrame:
    """
    Casts DataFrame columns to specified data types based on the provided schema.

    Parameters:
    - df (DataFrame): The input Spark DataFrame.
    - list_schema (list): A list of dictionaries with 'col_name' and 'data_type' keys.
    - logger (logging.Logger): Logger for logging information.

    Returns:
    - DataFrame: The transformed DataFrame with columns cast to specified types.
    
    Raises:
    - ValueError: If df or list_schema is None or list_schema is empty.
    """
    if df is None:
        raise ValueError("Input DataFrame 'df' cannot be None.")
    if not list_schema:
        raise ValueError("Input 'list_schema' cannot be None or empty.")
    
    # Create a set of existing columns for faster lookup
    existing_columns = set(df.columns)
    # Create a set of schema-defined columns
    schema_columns = set(col['col_name'] for col in list_schema if '#' not in col['col_name'])
    
    # Columns to add (present in schema but not in DataFrame)
    columns_to_add = schema_columns - existing_columns
    # Columns to drop (present in DataFrame but not in schema)
    columns_to_drop = existing_columns - schema_columns
    
    # Add missing columns with None (null) values
    for col_name in columns_to_add:
        df = df.withColumn(col_name, F.lit(None))
        logger.info(f"Added missing column '{col_name}' with null values.")
    
    # Drop extra columns not present in schema
    if columns_to_drop:
        df = df.drop(*columns_to_drop)
        for col_name in columns_to_drop:
            logger.info(f"Dropped column '{col_name}' as it's not present in the schema.")
    
    # Define type mapping
    type_mapping = {
        'int': IntegerType(),
        'integer': IntegerType(),
        'long': LongType(),
        'bigint': LongType(),
        'bool': BooleanType(),
        'boolean': BooleanType(),
        'double': DoubleType(),
        'float': DoubleType(),  # Using DoubleType for float compatibility
        'decimal': DoubleType(),  # Adjust as needed
        'real': DoubleType(),
        'money': DoubleType(),
        'currency': DoubleType(),
        'datetime': TimestampType(),
        'timestamp': TimestampType(),
        'date': DateType(),
        # Add more mappings as needed
    }
    
    # Iterate over schema and apply casts
    for column in list_schema:
        col_name = column.get('col_name')
        desired_type_str = column.get('data_type', 'string').lower()
        desired_type = type_mapping.get(desired_type_str, StringType())
        
        if col_name not in df.columns:
            logger.warning(f"Column '{col_name}' is missing in DataFrame even after addition.")
            continue  # Skip casting for missing columns
        
        current_type_str = dict(df.dtypes).get(col_name, '').lower()
        
        # Determine if casting is needed
        need_cast = False
        if isinstance(desired_type, IntegerType) and 'int' not in current_type_str:
            need_cast = True
        elif isinstance(desired_type, LongType) and 'int' not in current_type_str:
            need_cast = True
        elif isinstance(desired_type, BooleanType) and 'bool' not in current_type_str:
            need_cast = True
        elif isinstance(desired_type, DoubleType) and current_type_str not in ['double', 'float', 'decimal', 'real', 'money', 'currency']:
            need_cast = True
        elif isinstance(desired_type, DateType) and 'date' not in current_type_str:
            need_cast = True
        elif isinstance(desired_type, TimestampType) and 'timestamp' not in current_type_str and 'datetime' not in current_type_str:
            need_cast = True
        elif isinstance(desired_type, StringType) and current_type_str != 'string':
            need_cast = True
        
        if need_cast:
            try:
                df = df.withColumn(col_name, F.col(col_name).cast(desired_type))
                logger.info(f"Converted column '{col_name}' from '{current_type_str}' to '{desired_type.simpleString()}'.")
            except Exception as e:
                logger.error(f"Failed to cast column '{col_name}' to '{desired_type.simpleString()}': {e}")
        else:
            logger.info(f"No casting needed for column '{col_name}' (current type: '{current_type_str}').")
    
    return df

def get_columns_partitioned_by_schema(list_schema: list) -> list:
    lst_return = []
    for dcol in list_schema:
        if dcol['comment'].get('partition',{}).get('enabled', False):
            lst_return.append(dcol)

    lst_return.sort(key=lambda x: x['comment']['partition']['order_sort'])
    return [col['col_name'] for col in lst_return]