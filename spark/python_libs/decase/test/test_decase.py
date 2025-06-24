import pytest
from unittest import mock
from unittest.mock import MagicMock, patch
import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType
from pyspark.sql import Row
import json

from decase import get_spark_session, check_minio_prefix_exists

def test_get_spark_session():
    app_name = "TestApp"
    spark = get_spark_session(app_name)
    assert isinstance(spark, SparkSession)
    assert spark.sparkContext.appName == app_name