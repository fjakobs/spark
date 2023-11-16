
from pathlib import Path

cwd = Path(__file__).parent

import pandas as pd
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

from pyspark.sql.functions import col, wasm_udf
from pyspark.sql.types import LongType

x = pd.Series([1, 2, 3])

# Create a Spark DataFrame, 'spark' is an existing SparkSession
df = spark.createDataFrame(pd.DataFrame(x, columns=["x"]))

multiply = wasm_udf("multiply", cwd / "multiply.wasm", returnType="long")

df = df.select(multiply(col("x"), col("x")), "x")

# print plan
df.explain(True)
df.show()
