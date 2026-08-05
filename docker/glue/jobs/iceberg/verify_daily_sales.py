from awsglue.context import GlueContext
from pyspark.context import SparkContext

spark_context = SparkContext.getOrCreate()
glue_context = GlueContext(spark_context)
spark = glue_context.spark_session

spark.sparkContext.setLogLevel("WARN")

# spark.sql("SELECT DISTINCT batch_id,batch_date FROM local.silver.sales_order_items_enriched").show(truncate=False)
# spark.sql("SELECT DISTINCT batch_id,batch_date FROM local.gold.daily_sales_summary").show(truncate=False)
spark.sql("SELECT * FROM local.silver.sales_order_items_enriched.snapshots").show(truncate=False)
#spark.sql("SELECT * FROM local.gold.daily_sales_summary.snapshots").show(truncate=False)

# TABLE_NAME="local.gold.daily_sales_summary"

# spark.sql("SHOW TABLES IN local.gold").show(truncate=False)

# spark.sql(f"DESCRIBE TABLE {TABLE_NAME}").show(100, truncate=False)

# spark.sql(f"SELECT * FROM {TABLE_NAME} LIMIT 20").show(truncate=False)

# spark.sql(f"SELECT count(*) FROM {TABLE_NAME}").show(truncate=False)

# #Snapshots
# spark.sql(
#     f"""
#     SELECT
#         committed_at,
#         snapshot_id,
#         parent_id,
#         operation
#     FROM {TABLE_NAME}.snapshots
#     ORDER BY committed_at DESC
#     """
# ).show(truncate=False)

# #History
# spark.sql(
#     f"""
#     SELECT *
#     FROM {TABLE_NAME}.history
#     ORDER BY made_current_at DESC
#     """
# ).show(truncate=False)
    
# #Data files
# spark.sql(
#     f"""
#     SELECT
#         file_path,
#         file_format,
#         record_count,
#         file_size_in_bytes
#     FROM {TABLE_NAME}.files
#     """
# ).show(truncate=False)

# #Partitions
# spark.sql(
#     f"""
#     SELECT *
#     FROM {TABLE_NAME}.partitions
#     """
# ).show(truncate=False)    