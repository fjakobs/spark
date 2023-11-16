from pyspark.sql import SparkSession
# spark = SparkSession.builder.getOrCreate()
spark = SparkSession.builder.remote("sc://localhost").getOrCreate()

from pyspark.sql.connect.functions import col, pandas_udf
from pyspark.sql.connect.types import LongType
import pandas as pd

def multiply_func(a: pd.Series, b: pd.Series) -> pd.Series:
    return a * b

multiply = pandas_udf(multiply_func, returnType=LongType())

x = pd.Series([1, 2, 3])

# Create a Spark DataFrame, 'spark' is an existing SparkSession
df = spark.createDataFrame(pd.DataFrame(x, columns=["x"]))

# df.withColumn('x', pandas_plus_one(df.v))
df = df.select(multiply(col("x"), col("x")), "x")

# print plan
df.explain(True)
df.show()
