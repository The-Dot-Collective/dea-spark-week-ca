import pytest
from pyspark import SparkContext

from spark_practices.day1_spark_tasks import double_rdd

double_testdata = [
    ([1, 3, 4, 7, 10], [2, 6, 8, 14, 20]),
    ([5, 7, 8], [10, 14, 16]),
]
double_testdata_ids = ["big list case", "smaller list case"]


@pytest.mark.parametrize(
    "integers_list, result_list", double_testdata, ids=double_testdata_ids
)
def test_double_rdd(integers_list: [int], result_list: [int], context: SparkContext):
    rdd = context.parallelize(integers_list)
    new_rdd = double_rdd(rdd)
    assert new_rdd.collect() == result_list
