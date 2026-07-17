from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import functions as F

BRONZE_INPUT_BASE_PATH=(
    "/home/hadoop/workspace/docker/glue/output/bronze/customers/"
)

spark_context=SparkContext.getOrCreate()
glue_context=GlueContext(spark_context)
spark=glue_context.spark_session

spark.sparkContext.setLogLevel("WARN")

df=spark.read.parquet(BRONZE_INPUT_BASE_PATH)

df.show(5)