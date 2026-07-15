from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import functions as F

from common.jdbc import read_sql_server_table

BRONZE_OUTPUT_PATH=(
    "/home/hadoop/workspace/docker/glue/output/bronze/orders"
)

def main()->None:
    spark_context=SparkContext.getOrCreate()
    glue_context=GlueContext(spark_context)
    spark=glue_context.spark_session

    spark.sparkContext.setLogLevel("WARN")

    orders=read_sql_server_table(spark=spark,table_name="sales.orders")

    bronze_orders=(
        orders
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("source_table",F.lit("sales.orders"))
        .withColumn("source_system",F.lit("SQLSERVER"))
    )

    print("Bronze table orders schema:")
    bronze_orders.printSchema()

    source_count = bronze_orders.count()
    print(f"Source count: {source_count}")

    bronze_orders.write.mode("overwrite").parquet(BRONZE_OUTPUT_PATH)

    written_orders = spark.read.parquet(
        BRONZE_OUTPUT_PATH
    )

    target_count=written_orders.count()
    print(f"Written file count: {target_count}")

    if source_count != target_count:
        raise ValueError(
            "Bronze row-count validation failed: "
            f"source={source_count}, target={target_count}"
        )

    print("Bronze row-count validation successful.")

    written_orders.show(5, truncate=False)

if __name__ == "__main__":
    main()