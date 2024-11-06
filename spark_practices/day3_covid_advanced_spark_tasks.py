import sys
from pathlib import Path

from py4j.protocol import NULL_TYPE
from pyspark import SparkConf

# from pyspark.sql.connect.dataframe import DataFrame
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    avg,
    dense_rank,
    lag,
    month,
    rank,
    row_number,
    sum,
    to_date,
    year,
)
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

LOCAL_DATA_PATH = Path(
    "/Users/carlosarocha/repos/dea-spark-week-ca/data/covid_data.csv"
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


# Task 1: Calculating Rolling Averages for New Cases and Deaths
def new_cases_rolling_average(df, window_range: int = 6):
    # df.printSchema()
    win_sec = (
        Window.partitionBy("location")
        .orderBy("date")
        .rowsBetween(-window_range, Window.currentRow)
    )
    df_selected = df.select(
        df.location.alias("country"),
        df.date,
        df.new_cases,
        avg("new_cases").over(win_sec).alias("weekly_avg_cases"),
        df.new_deaths,
        avg("new_deaths").over(win_sec).alias("weekly_avg_deaths"),
    )
    final_df = df_selected.filter(df_selected.weekly_avg_deaths > 0)
    final_df.show(50)


# Task 2: Calculating Monthly Total Cases and Deaths per Continent
def total_cases_by_continent(df):
    df_selected = df.select(
        df.continent,
        year(df.date).alias("year"),
        month(df.date).alias("month"),
        df.new_cases,
        df.new_deaths,
    )
    df_totals = (
        df_selected.groupBy("continent", "year", "month")
        .agg(sum(df_selected.new_cases), sum(df_selected.new_deaths))
        .orderBy("continent", "year", "month")
    )
    df_totals.filter(df_totals.continent != NULL_TYPE).show()


# Task 3: Identifying the Top 5 Countries with the Highest Death Rate per Million
# For each country, calculate the death rate per million people (using total_deaths_per_million).
# Rank the countries by this metric, showing the top 5 countries with the highest death rates.
# Include the columns location, total_deaths_per_million, and rank.
# Hints:
# Use rank() window function to rank countries by total_deaths_per_million.
# Limit the result to the top 5 countries.
def top_five_highest_rate_countries(df):
    pass


# Task 4: Comparing COVID-19 Testing Rates Between Countries with Different GDP Levels
# Divide the countries into high GDP and low GDP groups based on the median GDP per capita.
# Calculate the average number of tests per million for each group.
# Show the results for both high and low GDP groups.
# Hints:
# Use approxQuantile() to find the median GDP per capita.
# Use groupBy() and agg() to perform the comparison.
def comparing_testing_rates(df):
    pass


# Task 5: Analyzing Excess Mortality Trends
# For each country, calculate the cumulative excess mortality over time.
# Filter the results to show only countries with a population greater than 50 million.
# Display the location, date, and excess_mortality_cumulative columns, sorted by country and date.
# Hints:
# Use a running total calculation for cumulative excess mortality.
# Apply a filter condition on the population column.
def excess_mortality_trends_analysis(df):
    pass


# Task 6: Performing an Outer Join Between Testing and Vaccination Data
# Create two DataFrames: one containing location, total_tests, and date, and the other containing location, total_vaccinations, and date.
# Perform an outer join on the location and date columns to merge the two DataFrames.
# Show the results, including the location, date, total_tests, and total_vaccinations columns.
# Hints:
# Use the join() method with the how='outer' argument for the join operation.
def testing_vaccination_outer_joint(df):
    pass


# Bonus Task: Using Spark SQL to Identify Correlation Between Testing and Vaccination Rates
# Register the COVID-19 DataFrame as a SQL view.
# Write a SQL query that calculates the correlation coefficient between the number of tests and vaccinations across all countries.
# Show the correlation result.
# Hints:
# Use the corr() function in Spark SQL to calculate the correlation between two variables.
def testing_vaccination_correlation(df):
    pass


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
        "1": "new_cases_rolling_average(df)",
        "2": "total_cases_by_continent(df)",
        "3": "top_five_highest_rate_countries(df)",
        "4": "comparing_testing_rates(df)",
        "5": "excess_mortality_trends_analysis(df)",
        "6": "testing_vaccination_outer_joint(df)",
        "7": "testing_vaccination_correlation(df)",
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

    """
    # Sample data
    data = [("Alice", 5000, "2023-01-01"), ("Bob", 4500, "2023-02-01"), ("Alice", 5500, "2023-03-01")]
    df = spark.createDataFrame(data, ["Name", "Salary", "Date"])
    df.show()

    # Complex filter and multi-column aggregation
    df_filtered = df.filter(df.Salary >= 4500)
    df_agg = df_filtered.groupBy("Name").agg(
        avg("Salary").alias("avg_salary"),
        sum("Salary").alias("total_salary")
    )

    df_agg.show()

    # Sample data
    data1 = [("Alice", 1), ("Bob", 2)]
    data2 = [("Alice", 1), ("Charlie", 3)]
    df1 = spark.createDataFrame(data1, ["Name", "ID"])
    df2 = spark.createDataFrame(data2, ["Name", "ID"])

    # Outer join
    df_outer = df1.join(df2, "ID", "outer")
    df_outer.show()

    # Anti join (finds records in df1 that are not in df2)
    df_anti = df1.join(df2, "ID", "left_anti")
    df_anti.show()

    # Sample time-series data
    data = [("2023-01-01", 100), ("2023-01-02", 110), ("2023-01-03", 105)]
    df = spark.createDataFrame(data, ["Date", "Value"])

    # Define window specification
    window_spec = Window.orderBy("Date")

    # Calculate change in value using lag
    df.withColumn("Previous_Value", lag("Value", 1).over(window_spec)) \
        .withColumn("Change", df.Value - lag("Value", 1).over(window_spec)) \
        .show()

    # Sample data
    data = [("Alice", "Sales", 5000), ("Bob", "Sales", 4500), ("Charlie", "HR", 4000)]
    df = spark.createDataFrame(data, ["Name", "Department", "Salary"])
    df.show()
    # Multi-level aggregation with rollup
    df.rollup("Department", "Name").sum("Salary").show()

    # Define a schema for the data
    schema = StructType([
        StructField("Name", StringType(), True),
        StructField("Age", IntegerType(), True)
    ])

    # Create a DataFrame with an explicit schema
    data = [("Alice", 25), ("Bob", 30)]
    df = spark.createDataFrame(data, schema=schema)

    df.printSchema()
    df.show()"""

    spark.stop()
