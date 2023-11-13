
import sys
from pathlib import Path

cwd = Path(__file__).parent
sys.path.append(cwd.parent / "python")


from pyspark.sql import SparkSession
spark = SparkSession.builder.remote("sc://localhost").getOrCreate()

#print(spark.udf.registerWasm)
spark.udf.registerWasm("add", cwd / "add.wasm")