"""Spark sessions practices Day 2"""

# import time
import sys
from pathlib import Path

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

LOCAL_DATA_PATH = Path(
    "/Users/carlosarocha/repos/dea-spark-week-ca/data/covid_data.csv"
)
SAVING_PATH = Path(
    "/Users/carlosarocha/repos/dea-spark-week-ca/output/covid_gdp_analysis.parquet"
)


# Create a SparkConf object
conf = (
    SparkConf()
    .setAppName("YourAppName")
    .set("spark.driver.extraJavaOptions", "-XX:ReservedCodeCacheSize=256m")
    .set("spark.executor.extraJavaOptions", "-XX:ReservedCodeCacheSize=256m")
    .set("spark.driver.memory", "2g")
    .set("spark.executor.memory", "2g")
)


# Task 1: Setting Up Spark Session for COVID-19 Data
def show_dataframe_info(df):
    df.printSchema()
    df.show(5, truncate=True)


# Task 2: Data Exploration with DataFrames
def covid_data_by_country(df, country):
    df_selection = df.select("location", "date", "total_cases", "total_deaths")
    df_filtered = df_selection.filter(df_selection["location"] == country).sort(
        df_selection["date"].desc()
    )
    df_filtered.show(5)


def covid_data_by_country_sql(df, spark, country):
    df.createOrReplaceTempView("covid_table")
    result = spark.sql(
        f"""SELECT location, date, total_cases, total_deaths
                          FROM covid_table
                          WHERE location = '{country}'
                          ORDER BY date desc"""
    )
    result.show(5)


# Task 3: Aggregating COVID-19 Data
def covid_totals_by_continent(df):
    df_selection = df.select("continent", "total_cases", "total_deaths")
    df_selection.groupby("continent").max().show()


def covid_totals_by_continent_sql(df, spark):
    df.createOrReplaceTempView("covid_table")
    result = spark.sql(
        """  SELECT continent, MAX(total_cases) as Total_cases, MAX(total_deaths) as Total_deaths
                            FROM covid_table
                            GROUP BY continent
                            HAVING continent IS NOT NULL"""
    )
    result.show()


# Task 4: Joining DataFrames
def joined_dataframe_by_country(df):
    df_first = df.select(df.iso_code, df.location.alias("country"), df.gdp_per_capita)
    df_joined = df_first.join(
        df[["iso_code", "total_cases", "total_deaths"]], how="left", on="iso_code"
    )
    df_final = (
        df_joined.groupBy("iso_code", "country", "gdp_per_capita")
        .agg(
            F.max("total_cases").alias("total_cases"),
            F.max("total_deaths").alias("total_deaths"),
        )
        .orderBy(df_joined.country.asc())
    )
    # df_joined.sort(df_joined['total_deaths'].desc()).drop_duplicates(['location']).show(5)
    df_final.show()
    return df_final


def joined_dataframe_by_country_sql(df, spark):
    df.createOrReplaceTempView("covid_table")
    result = spark.sql(
        """  SELECT DISTINCT(d.iso_code), d.location, d.gdp_per_capita, c.total_cases, c.total_deaths
                            FROM covid_table AS d
                            LEFT JOIN covid_table AS c
                                ON d.iso_code = c.iso_code
                            ORDER BY c.total_deaths DESC
                            """
    )
    result.show()
    return result


# Task 5: Spark SQL Queries
def total_cases_by_country(df):
    df_filtered = df.filter((df.population > 100000000) & (df.stringency_index > 50))
    df_grouped = df_filtered.groupBy("location").agg(
        F.max("total_cases").alias("total_cases"),
        F.max("total_deaths").alias("total_deaths"),
    )
    df_grouped.sort(df_grouped["total_deaths"].desc()).show()


def total_cases_by_country_sql(df, spark):
    df.createOrReplaceTempView("covid_table")
    result = spark.sql(
        """  SELECT location as country, MAX(total_cases) as Total_cases, MAX(total_deaths) as Total_deaths
                            FROM (SELECT * FROM covid_table WHERE population > 100000000 AND stringency_index > 50)
                            GROUP BY country
                            ORDER BY Total_deaths desc"""
    )
    result.show()


# Task 6: Saving DataFrames to Disk
def save_dataframe_as_parquet(df):
    df_joined = joined_dataframe_by_country(df)
    df_joined.write.parquet(str(SAVING_PATH))
    print(f"Parquet file saved at:{SAVING_PATH}")


if __name__ == "__main__":

    spark = (
        SparkSession.builder.appName("Day2_COVID_Analysis")
        .config(conf=conf)
        .master("local[*]")
        .config("spark.sql.debug.maxToStringFields", 100)
        .getOrCreate()
    )
    df = spark.read.csv(str(LOCAL_DATA_PATH), header=True, inferSchema=True)

    functions = {
        "1": "show_dataframe_info(df)",
        "2": "covid_data_by_country(df, 'United States')",
        "2-sql": "covid_data_by_country_sql(df, spark, 'United States')",
        "3": "covid_totals_by_continent(df)",
        "3-sql": "covid_totals_by_continent_sql(df, spark)",
        "4": "joined_dataframe_by_country(df)",
        "4-sql": "joined_dataframe_by_country_sql(df, spark)",
        "5": "total_cases_by_country(df)",
        "5-sql": "total_cases_by_country_sql(df, spark)",
        "6": "save_dataframe_as_parquet(df)",
    }

    args = sys.argv
    if len(args) > 1:
        for arg in args[1:]:
            print(f"Task {arg} : **********************************")
            eval(f"{functions[arg]}")

    else:
        for key in functions.keys():
            print(f"Task {key} : **********************************")
            eval(f"{functions[key]}")

    spark.stop()
