"""Spark practices"""

import os
import sys
import time

from pyspark.sql import SparkSession


def start_spark_session():
    """Initialize a Spark session"""
    spark_session = (
        SparkSession.builder.appName("MyApp").master("local[*]").getOrCreate()
    )
    return spark_session


def task_1(session):
    """Checks the version of Spark installed in my environment."""
    os.system("pyspark --version")


def task_2(session):
    """Creating and manipulating RDDs"""
    sc = session.sparkContext
    rdd = sc.parallelize([1, 2, 3, 4, 5])
    double_rdd = rdd.map(lambda x: x * 2)
    print(double_rdd)
    print(f"Doubled RDD:{double_rdd.collect()}")


def task_3(session):
    """FlatMap and Filter on RDDs"""
    sc = session.sparkContext
    rdd = sc.parallelize(
        ["Apache Spark is fast", "It processes Big Data", "RDDs are resilient"]
    )
    flat_rdd = rdd.flatMap(lambda x: x.split())
    filter_rdd = flat_rdd.filter(lambda x: len(x) < 4)
    print(f"Filtered RDD:{filter_rdd.collect()}")


def task_4(session):
    """Lazy Evaluation"""
    sc = session.sparkContext
    rdd = sc.parallelize(list(range(1, 11)))
    double_rdd = rdd.map(lambda x: x * 2)
    filter_rdd = double_rdd.filter(lambda x: x > 10)
    print(f"Filtered RDD:{filter_rdd.collect()}")


def task_5(session):
    """Persistence for Performance"""
    sc = session.sparkContext
    rdd = sc.parallelize(list(range(1, 101)))
    filter_rdd = rdd.filter(lambda x: x % 2 == 0)
    filter_rdd.persist()
    start = time.time()
    print(f"Even numbers:{filter_rdd.count()}", f"time:{time.time() - start}")
    start = time.time()
    print(f"Even numbers:{filter_rdd.count()}", f"time:{time.time() - start}")


if __name__ == "__main__":

    Day1_SparkIntro = start_spark_session()
    args = sys.argv
    if len(args) > 1:
        for arg in args[1:]:
            print(f"Task {arg} : **********************************")
            eval(f"task_{arg}(Day1_SparkIntro)")


# Reflection Question 1:
# What is the difference between a transformation and an action in Spark?
# Transformation creates a new RDD from applying a function or a transformation to the data inside an already existing RDD. It creates an execution to the data that will run at the moment some action press the button, like asking for the resulting data with the collect() method, or counting the results with count() method.

# Reflection Question 2:
# What is lazy evaluation, and why does it improve Spark's performance?
# Lazy evaluation refers to what exactly I described before. The way that spark transform the data, creates first an execution that is triggered when you finally want the transformed data and call to collect it, a call that will trigger first the transformation.

# Reflection Question 3:
# How does persistence help in improving the performance of Spark jobs?
# Persistence saves the RDD data or previous execution in memory so future similar calls have quicker access to the information. Is like helping not to trigger a similar transformation again by a similar action, avoiding repeating calculations.
