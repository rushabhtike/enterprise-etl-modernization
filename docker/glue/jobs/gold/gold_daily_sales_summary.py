from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import functions as F

SILVER_INPUT_BASE_PATH=(
    "/home/hadoop/workspace/docker/glue/output/silver/"
)

GOLD_OUTPUT_BASE_PATH=(
    "/home/hadoop/workspace/docker/glue/output/gold/"
)

GOLD_DAILY_SALES_SUMMARY_PATH=(
    GOLD_OUTPUT_BASE_PATH+"daily_sales_summary/"
)

def main()->None:
    spark_context=SparkContext.getOrCreate()
    glue_context=GlueContext(spark_context)
    spark=glue_context.spark_session

    spark.sparkContext.setLogLevel("WARN")
    
    df=spark.read.parquet(SILVER_INPUT_BASE_PATH+"sales_order_items_enriched/")
    
    # Taking only delivered orders
    df_delivered = (
        df
        .filter(F.col("order_status")=="DELIVERED")
        .withColumn("sales_date",F.to_date(F.col("order_date")))
        .cache()
    )
    
    delivered_count = df_delivered.count()

    if delivered_count == 0:
        raise ValueError(
            "No DELIVERED records found for Gold processing."
        )
    
    # Derived columns
    df_delivered_agg = (
        df_delivered
        .groupBy("sales_date","category","customer_state","customer_country")
        .agg(
            F.countDistinct(F.col("order_id")).alias("total_orders"),
            F.count(F.col("order_item_id")).alias("total_order_items"),
            F.countDistinct(F.col("customer_id")).alias("unique_customers"),
            F.countDistinct(F.col("product_id")).alias("unique_products"),
            F.sum(F.col("quantity")).alias("units_sold"),
            F.sum(F.col("gross_line_amount")).alias("gross_sales"),
            F.sum(F.col("discount_amount")).alias("total_discount"),
            F.sum(F.col("net_line_amount")).alias("net_sales"),
        )
        .withColumn("average_item_value",F.round(F.col("net_sales") / F.col("total_order_items"),2))
        .withColumn("_gold_processed_timestamp", F.current_timestamp())
        .cache()
    )
    
    # Validation
    gold_stats = (
        df_delivered_agg
        .agg(
            F.sum(F.col("units_sold")).alias("gold_total_units_sold"),
            F.sum(F.col("gross_sales")).alias("gold_gross_sales"),
            F.sum(F.col("net_sales")).alias("gold_net_sales"),
        )
        .first()
    )

    silver_stats = (
        df_delivered
        .agg(
            F.sum(F.col("quantity")).alias("silver_total_units_sold"),
            F.sum(F.col("gross_line_amount")).alias("silver_gross_sales"),
            F.sum(F.col("net_line_amount")).alias("silver_net_sales"),
        )
        .first()   
    )
    
    if gold_stats["gold_total_units_sold"]!=silver_stats["silver_total_units_sold"]:
        raise ValueError(f"Units sold mismatch. Gold: {gold_stats['gold_total_units_sold']}, Silver: {silver_stats['silver_total_units_sold']}")
    if abs(gold_stats["gold_gross_sales"]-silver_stats["silver_gross_sales"])>0.02:
        raise ValueError(f"Gross sales mismatch. Gold: {gold_stats['gold_gross_sales']}, Silver: {silver_stats['silver_gross_sales']}")
    if abs(gold_stats["gold_net_sales"]-silver_stats["silver_net_sales"])>0.02:
        raise ValueError(f"Net sales mismatch. Gold: {gold_stats['gold_net_sales']}, Silver: {silver_stats['silver_net_sales']}")
    
    # Writing Gold table:
    
    df_delivered_agg.write.mode("overwrite").parquet(GOLD_DAILY_SALES_SUMMARY_PATH)
    
    sales_summary_count=df_delivered_agg.count()
    
    print(
        f"Gold Daily Sales Summary written to: "
        f"{GOLD_DAILY_SALES_SUMMARY_PATH}"
    )

    print(
        f"Final sales summary row count: "
        f"{sales_summary_count}"
    )
    
    print("Sample sales summary records:")

    (
        df_delivered_agg
        .select(
            "sales_date",
            "category",
            "customer_state",
            "customer_country",
            "total_orders",
            "total_order_items",
            "unique_customers",
            "unique_products",
            "units_sold",
            "gross_sales",
            "total_discount",
            "net_sales",
            "average_item_value"
        )
        .show(20, truncate=False)
    )
    
    df_delivered.unpersist()
    df_delivered_agg.unpersist()
    
    print(
        "Gold Daily Sales Summary processing "
        "completed successfully."
    )


if __name__ == "__main__":
    main()