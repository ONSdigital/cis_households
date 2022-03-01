from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType
from pyspark.sql.types import StringType


def generate_sample_proportional_to_size_udf(spark):
    def sample_proportional_to_size(imputation_variables, expected_frequencies, number):
        """UDF for sampling proportional to size."""
        import numpy as np

        results = np.random.choice(a=imputation_variables, p=expected_frequencies, replace=False, size=number).tolist()
        return results

    spark.udf.register("sample_proportional_to_size_udf_reg", sample_proportional_to_size)
    return F.udf(sample_proportional_to_size, ArrayType(StringType()))
