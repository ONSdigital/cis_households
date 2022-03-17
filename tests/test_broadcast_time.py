import timeit

from cishouseholds.pyspark_utils import get_or_create_spark_session
from tests.derive.test_assign_multigen import test_assign_multigeneration


def test_time_required(f):
    spark_session = get_or_create_spark_session()
    start = timeit.timeit()
    f(spark_session)
    end = timeit.timeit()
    print(f"-- Time required for is ", end - start)  # functional


if __name__ == "__main__":
    test_time_required(f=test_assign_multigeneration)
