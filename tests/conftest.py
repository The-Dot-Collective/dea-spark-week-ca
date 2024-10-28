import pytest
from pyspark import SparkContext
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def context() -> SparkContext:
    spark = SparkSession.builder.appName("MyApp").master("local[*]").getOrCreate()
    return spark.sparkContext
