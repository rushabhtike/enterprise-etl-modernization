from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

BRONZE_INPUT_BASE_PATH=(
    "/home/hadoop/workspace/docker/glue/output/bronze/order_items/"
)

SILVER_OUTPUT_BASE_PATH=(
    "/home/hadoop/workspace/docker/glue/output/silver/order_items/"
)

REJECT_BASE_PATH=(
    "/home/hadoop/workspace/docker/glue/output/reject/order_items/"
)

def clean_order_items(df: DataFrame)->DataFrame:
    return df

def add_data_quality_errors(df: DataFrame)->DataFrame:
    return (
    df.withColumn("dq_errors",
                  F.array_compact(
                        F.array(
                            F.when(F.col("order_item_id").isNull(), F.lit("ORDER_ITEM_ID_MISSING")),
                            F.when(F.col("order_id").isNull(), F.lit("ORDER_ID_MISSING")),
                            F.when(F.col("product_id").isNull(), F.lit("PRODUCT_ID_MISSING")),
                            F.when(F.col("quantity")<0, F.lit("QUANTITY < 0")),
                            F.when(F.col("unit_price")<0, F.lit("UNIT_PRICE < 0")),
                            F.when(F.col("discount_amount")<0, F.lit("DISCOUNT_AMOUNT < 0")),
                            F.when(F.col("line_total")<0, F.lit("LINE_TOTAL < 0")),
                            F.when((F.col("created_timestamp").isNull()) | (F.col("created_timestamp") == ""), F.lit("created_timestamp_MISSING")),
                            F.when((F.col("updated_timestamp").isNull()) | (F.col("updated_timestamp") == ""), F.lit("updated_timestamp_MISSING")),
                            F.when((F.col("source_system").isNull()) | (F.col("source_system") == ""), F.lit("source_system_MISSING")),
                            F.when((F.col("is_deleted").isNull()) | (F.col("is_deleted") == ""), F.lit("is_deleted_MISSING")),
                            F.when((F.col("ingestion_timestamp").isNull()) | (F.col("ingestion_timestamp") == ""), F.lit("ingestion_timestamp_MISSING")),
                            F.when((F.col("source_table").isNull()) | (F.col("source_table") == ""), F.lit("source_table_MISSING"))
                            )
                        )
                    )
    )

def main()->None:
    spark_context=SparkContext.getOrCreate()
    glue_context=GlueContext(spark_context)
    spark=glue_context.spark_session

    spark.sparkContext.setLogLevel("WARN")

    print(f"Reading order_items data from: {BRONZE_INPUT_BASE_PATH}")

    bronze_order_items=spark.read.parquet(BRONZE_INPUT_BASE_PATH)

    bronze_order_items.printSchema()
    source_count = bronze_order_items.count()
    print(f"Bronze order_items count: {source_count}")

    validated_order_items=(
        bronze_order_items
        .transform(clean_order_items)
        .transform(add_data_quality_errors)
        .cache()
    )

    valid_order_items=(
        validated_order_items
        .filter(F.size(F.col("dq_errors"))==0)
        .drop("dq_errors")
        .withColumn("_silver_processed_timestamp",F.current_timestamp())
    )

    rejected_order_items=(
        validated_order_items
        .filter(F.size(F.col("dq_errors"))>0)
        .withColumn("_rejected_timestamp",F.current_timestamp())
    )
    
    valid_order_items_count=valid_order_items.count()
    rejected_order_items_count=rejected_order_items.count()
    print(f"Valid order_items count is: {valid_order_items_count}")
    print(f"Rejected order_items count is: {rejected_order_items_count}")
    
    if source_count!=valid_order_items_count+rejected_order_items_count:
        raise ValueError(
            "Order_items reconciliation failed: "
            f"source={source_count}"
            f"valid={valid_order_items_count}"
            f"reject={rejected_order_items_count}"
        )
    
    valid_order_items.write.mode("overwrite").parquet(
        SILVER_OUTPUT_BASE_PATH
    )

    rejected_order_items.write.mode("overwrite").parquet(
        REJECT_BASE_PATH
    )

    written_file_count=spark.read.parquet(SILVER_OUTPUT_BASE_PATH).count()
    rejected_file_count=spark.read.parquet(REJECT_BASE_PATH).count()

    print(
        "Silver order_items written to:"
        f"{SILVER_OUTPUT_BASE_PATH}\n"
        "Count: "
        f"{written_file_count}"
    )

    print(
        "Rejected order_items written to:"
        f"{REJECT_BASE_PATH}\n"
        "Count: "
        f"{rejected_file_count}"
    )

    print("Data quality error summary:")
    (
        rejected_order_items
        .select(
            F.explode("dq_errors").alias("dq_error")
        )
        .groupBy("dq_error")
        .count()
        .orderBy(F.desc("count"))
        .show(truncate=False)
    )

    print("Sample valid order_items:")
    valid_order_items.show(10,truncate=False)

    print(
        "Silver order_items processing completed "
        "successfully."
    )

    validated_order_items.unpersist()

if __name__=="__main__":
    main()