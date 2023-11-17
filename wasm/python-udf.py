import timeit
import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import LongType

spark = SparkSession.builder.getOrCreate()

def multiply(a, b):
    return a * b

multiply = udf(multiply, returnType=LongType())

df = spark.range(1000 * 1000)
df = df.withColumnRenamed("id", "x")
df = df.withColumn("y", col("x") * 2)
df = df.select(multiply(col("x"), col("y")), "x", "y")

# print plan
df.printSchema()
df.explain(True)

start_time = timeit.default_timer()

result = df.collect()

end_time = timeit.default_timer()

print(f"Executed the code in: {end_time - start_time} seconds")

# spark.createDataFrame(result).show(20)

