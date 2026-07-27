from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

BRONZE_INPUT_BASE_PATH=(
    "/home/hadoop/workspace/docker/glue/output/bronze/orders/"
)

SILVER_OUTPUT_BASE_PATH=(
    "/home/hadoop/workspace/docker/glue/output/silver/orders/"
)

REJECT_BASE_PATH=(
    "/home/hadoop/workspace/docker/glue/output/reject/orders/"
)

def clean_orders(df: DataFrame)->DataFrame:
    return (
        df
        .withColumn("order_status",F.trim(F.col("order_status")))
        .withColumn("payment_method",F.trim(F.col("payment_method")))
        .withColumn("source_system",F.trim(F.initcap(F.col("source_system"))))
    )

def add_data_quality_errors(df: DataFrame)->DataFrame:
    return (
    df.withColumn("dq_errors",
                  F.array_compact(
                        F.array(
                            F.when(F.col("order_id").isNull(), F.lit("ORDER_ID_MISSING")),
                            F.when(F.col("customer_id").isNull(), F.lit("CUSTOMER_ID_MISSING")),
                            F.when(F.col("store_id").isNull(), F.lit("STORE_ID_MISSING")),
                            F.when(F.col("order_date").isNull(), F.lit("ORDER_DATE_MISSING")),
                            F.when((F.col("order_status").isNull()) | (F.col("order_status") == ""), F.lit("ORDER_STATUS_MISSING")),
                            F.when(~F.col("order_status").isin("PROCESSING","DELIVERED","PENDING","SHIPPED","CANCELLED"), F.lit("INVALID_ORDER_STATUS")),
                            F.when((F.col("payment_method").isNull()) | (F.col("payment_method") == ""), F.lit("PAYMENT_METHOD_MISSING")),
                            F.when(~F.col("payment_method").isin("UPI","CASH","DEBIT_CARD","CREDIT_CARD","NET_BANKING"), F.lit("INVALID_PAYMENT_METHOD")),
                            F.when(F.col("order_total").isNull(), F.lit("ORDER_TOTAL_MISSING")),
                            F.when(F.col("order_total")<0, F.lit("ORDER_TOTAL < 0")),
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

    print(f"Reading orders data from: {BRONZE_INPUT_BASE_PATH}")

    bronze_orders=spark.read.parquet(BRONZE_INPUT_BASE_PATH)

    bronze_orders.printSchema()
    source_count = bronze_orders.count()
    print(f"Bronze orders count: {source_count}")

    validated_orders=(
        bronze_orders
        .transform(clean_orders)
        .transform(add_data_quality_errors)
        .cache()
    )

    valid_orders=(
        validated_orders
        .filter(F.size(F.col("dq_errors"))==0)
        .drop("dq_errors")
        .withColumn("_silver_processed_timestamp",F.current_timestamp())
    )

    rejected_orders=(
        validated_orders
        .filter(F.size(F.col("dq_errors"))>0)
        .withColumn("_rejected_timestamp",F.current_timestamp())
    )
    
    valid_orders_count=valid_orders.count()
    rejected_orders_count=rejected_orders.count()
    print(f"Valid orders count is: {valid_orders_count}")
    print(f"Rejected orders count is: {rejected_orders_count}")
    
    if source_count!=valid_orders_count+rejected_orders_count:
        raise ValueError(
            "Orders reconciliation failed: "
            f"source={source_count}"
            f"valid={valid_orders_count}"
            f"reject={rejected_orders_count}"
        )
    
    valid_orders.write.mode("overwrite").parquet(
        SILVER_OUTPUT_BASE_PATH
    )

    rejected_orders.write.mode("overwrite").parquet(
        REJECT_BASE_PATH
    )

    written_file_count=spark.read.parquet(SILVER_OUTPUT_BASE_PATH).count()
    rejected_file_count=spark.read.parquet(REJECT_BASE_PATH).count()

    print(
        "Silver orders written to:"
        f"{SILVER_OUTPUT_BASE_PATH}\n"
        "Count: "
        f"{written_file_count}"
    )

    print(
        "Rejected orders written to:"
        f"{REJECT_BASE_PATH}\n"
        "Count: "
        f"{rejected_file_count}"
    )

    print("Data quality error summary:")
    (
        rejected_orders
        .select(
            F.explode("dq_errors").alias("dq_error")
        )
        .groupBy("dq_error")
        .count()
        .orderBy(F.desc("count"))
        .show(truncate=False)
    )

    print("Sample valid orders:")
    valid_orders.show(10,truncate=False)

    print(
        "Silver orders processing completed "
        "successfully."
    )

    validated_orders.unpersist()

if __name__=="__main__":
    main()