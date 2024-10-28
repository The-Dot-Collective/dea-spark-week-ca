from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from pyspark.testing import assertDataFrameEqual

from spark_practices.day2_covid_spark_tasks import joined_dataframe_by_country


def test_joined_dataframe_by_country():
    spark = SparkSession.builder.getOrCreate()

    input_df = spark.createDataFrame(
        data=[
            ("GB", "EU", 10, 20, 60),
            ("GB", "EU", 10, 10, 50),
        ],
        schema=StructType(
            [
                StructField("iso_code", dataType=StringType()),
                StructField("location", dataType=StringType()),
                StructField("gdp_per_capita", dataType=IntegerType()),
                StructField("total_cases", dataType=IntegerType()),
                StructField("total_deaths", dataType=IntegerType()),
            ]
        ),
    )

    expected_df = spark.createDataFrame(
        data=[("GB", "EU", 10, 20, 60)],
        schema=StructType(
            [
                StructField("iso_code", dataType=StringType()),
                StructField("location", dataType=StringType()),
                StructField("gdp_per_capita", dataType=IntegerType()),
                StructField("total_cases", dataType=IntegerType()),
                StructField("total_deaths", dataType=IntegerType()),
            ]
        ),
    )
    assertDataFrameEqual(joined_dataframe_by_country(input_df), expected_df)
