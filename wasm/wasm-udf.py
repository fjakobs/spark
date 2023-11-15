
import sys
from pathlib import Path

cwd = Path(__file__).parent
sys.path.append(cwd.parent / "python")

import pandas as pd
from pyspark.sql import SparkSession
spark = SparkSession.builder.remote("sc://localhost").getOrCreate()

from pyspark.sql.connect.functions import col, wasm_udf
from pyspark.sql.connect.types import LongType

x = pd.Series([1, 2, 3])

# Create a Spark DataFrame, 'spark' is an existing SparkSession
df = spark.createDataFrame(pd.DataFrame(x, columns=["x"]))

multiply = wasm_udf("multiply", cwd / "multiply.wasm", returnType="long")

df = df.select(multiply(col("x"), col("x")), "x")
#df = df.select("x")

# print plan
df.explain()
# df.show()
