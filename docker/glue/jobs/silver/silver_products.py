from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

BRONZE_INPUT_BASE_PATH=(
    "/home/hadoop/workspace/docker/glue/output/bronze/products/"
)

SILVER_OUTPUT_BASE_PATH=(
    "/home/hadoop/workspace/docker/glue/output/silver/products/"
)

REJECT_BASE_PATH=(
    "/home/hadoop/workspace/docker/glue/output/reject/products/"
)

def clean_products(df: DataFrame)->DataFrame:
    return (
        df
        .withColumn("product_name",F.trim(F.initcap(F.col("product_name"))))
        .withColumn("category",F.trim(F.initcap(F.col("category"))))
        .withColumn("subcategory",F.trim(F.initcap(F.col("subcategory"))))
        .withColumn("brand",F.trim(F.initcap(F.col("brand"))))
        .withColumn("source_system",F.trim(F.initcap(F.col("source_system"))))
    )

def add_data_quality_errors(df: DataFrame)->DataFrame:
    return (
    df.withColumn("dq_errors",
                  F.array_compact(
                        F.array(
                            F.when(F.col("product_id").isNull(), F.lit("PRODUCT_ID_MISSING")),
                            F.when(F.col("supplier_id").isNull(), F.lit("SUPPLIER_ID_MISSING")),
                            F.when((F.col("product_name").isNull()) | (F.col("product_name") == ""), F.lit("PRODUCT_NAME_MISSING")),
                            F.when((F.col("category").isNull()) | (F.col("category") == ""), F.lit("CATEGORY_MISSING")),
                            F.when((F.col("subcategory").isNull()) | (F.col("subcategory") == ""), F.lit("SUBCATEGORY_MISSING")),
                            F.when((F.col("brand").isNull()) | (F.col("brand") == ""), F.lit("BRAND_MISSING")),
                            F.when((F.col("unit_price").isNull()) | (F.col("unit_price") == ""), F.lit("UNIT_PRICE_MISSING")),
                            F.when((F.col("unit_price")<=0) , F.lit("UNIT_PRICE_<=0")),
                            F.when((F.col("cost_price").isNull()) | (F.col("cost_price") == ""), F.lit("COST_PRICE_MISSING")),
                            F.when((F.col("cost_price")>F.col("unit_price")) , F.lit("UNIT_PRICE_LESS_THAN_COST_PRICE")),
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

    print(f"Reading products data from: {BRONZE_INPUT_BASE_PATH}")

    bronze_products=spark.read.parquet(BRONZE_INPUT_BASE_PATH)

    bronze_products.printSchema()
    source_count = bronze_products.count()
    print(f"Bronze products count: {source_count}")

    validated_products=(
        bronze_products
        .transform(clean_products)
        .transform(add_data_quality_errors)
        .cache()
    )

    valid_products=(
        validated_products
        .filter(F.size(F.col("dq_errors"))==0)
        .drop("dq_errors")
        .withColumn("_silver_processed_timestamp",F.current_timestamp())
    )

    rejected_products=(
        validated_products
        .filter(F.size(F.col("dq_errors"))>0)
        .withColumn("_rejected_timestamp",F.current_timestamp())
    )
    
    valid_products_count=valid_products.count()
    rejected_products_count=rejected_products.count()
    print(f"Valid products count is: {valid_products_count}")
    print(f"Rejected products count is: {rejected_products_count}")
    
    if source_count!=valid_products_count+rejected_products_count:
        raise ValueError(
            "product reconciliation failed: "
            f"source={source_count}"
            f"valid={valid_products_count}"
            f"reject={rejected_products_count}"
        )
    
    valid_products.write.mode("overwrite").parquet(
        SILVER_OUTPUT_BASE_PATH
    )

    rejected_products.write.mode("overwrite").parquet(
        REJECT_BASE_PATH
    )

    written_file_count=spark.read.parquet(SILVER_OUTPUT_BASE_PATH).count()
    rejected_file_count=spark.read.parquet(REJECT_BASE_PATH).count()

    print(
        "Silver products written to:"
        f"{SILVER_OUTPUT_BASE_PATH}\n"
        "Count: "
        f"{written_file_count}"
    )

    print(
        "Rejected products written to:"
        f"{REJECT_BASE_PATH}\n"
        "Count: "
        f"{rejected_file_count}"
    )

    print("Data quality error summary:")
    (
        rejected_products
        .select(
            F.explode("dq_errors").alias("dq_error")
        )
        .groupBy("dq_error")
        .count()
        .orderBy(F.desc("count"))
        .show(truncate=False)
    )

    print("Sample valid products:")
    valid_products.show(10,truncate=False)

    print(
        "Silver products processing completed "
        "successfully."
    )

    validated_products.unpersist()

if __name__=="__main__":
    main()