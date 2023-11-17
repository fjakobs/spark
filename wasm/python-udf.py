from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

from pyspark.sql.functions import col, udf
from pyspark.sql.types import LongType
import pandas as pd

def multiply_func(a, b):
    return a * b

multiply = udf(multiply_func, returnType=LongType())

x = pd.Series([1, 2, 3])

# Create a Spark DataFrame, 'spark' is an existing SparkSession
df = spark.createDataFrame(pd.DataFrame(x, columns=["x"]))
df = df.withColumn("y", col("x") * 2)
df = df.select(multiply(col("x"), col("y")), "x", "y")

# print plan
df.explain(True)
df.show()
