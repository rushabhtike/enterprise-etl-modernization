from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import functions as F

from common.jdbc import read_sql_server_table

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

    print("Sample records")
    bronze_orders.show(truncate=False)

    row_count = bronze_orders.count()
    print(f"Count: {row_count}")
    
if __name__ == "__main__":
    main()