from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

SILVER_INPUT_BASE_PATH=(
    "/home/hadoop/workspace/docker/glue/output/silver/"
)
SILVER_REJECT_BASE_PATH=(
    "/home/hadoop/workspace/docker/glue/output/reject/"
)

def check_key_uniqueness(df:DataFrame, col:str, table_name:str):
    stats=(
        df
        .agg(
            F.count(F.col(col)).alias("total_count"),
            F.countDistinct(F.col(col)).alias("distinct_count"),
            F.count("*").alias("row_count")         
        )
        .first()
    )
    if stats["total_count"] != stats["distinct_count"]:
        raise ValueError(f"Duplicate key error in '{table_name}'.")
    if stats["total_count"] != stats["row_count"]:
        raise ValueError(f"Null primary key found in '{table_name}'.")
    
    print(
            f"Primary key validation passed for "
            f"{table_name}.{col}"
        )

def main()->None:

    spark_context=SparkContext.getOrCreate()
    glue_context=GlueContext(spark_context)
    spark=glue_context.spark_session

    spark.sparkContext.setLogLevel("WARN")

    customers_df=spark.read.parquet(SILVER_INPUT_BASE_PATH+"/customers/")
    products_df=spark.read.parquet(SILVER_INPUT_BASE_PATH+"/products/")
    orders_df=spark.read.parquet(SILVER_INPUT_BASE_PATH+"/orders/")
    order_items_df=spark.read.parquet(SILVER_INPUT_BASE_PATH+"/order_items/")
    
    pk_dict = {
    "customers": (customers_df, "customer_id"),
    "products": (products_df, "product_id"),
    "orders": (orders_df, "order_id"),
    "order_items": (order_items_df, "order_item_id"),
}

    #Check for primary key uniqueness
    for table_name, (df,col) in pk_dict.items():
        check_key_uniqueness(df,col,table_name)

    customers_count=customers_df.count()
    products_count=products_df.count()
    orders_count=orders_df.count()
    order_items_count=order_items_df.count()

    print(f"Customers count: {customers_count}")
    print(f"Products count: {products_count}")
    print(f"Orders count: {orders_count}")
    print(f"Order Items count: {order_items_count}")

    # Detecting orphan records
    
    # Orders whose customer does not exist.
    order_customer_rej = (
        orders_df
        .join(customers_df,"customer_id",how="left_anti")
        .withColumn("_relationship_error", F.lit("CUSTOMER NOT FOUND"))
        .withColumn("_rejected_timestamp", F.current_timestamp())
    )  
        
    # Keep only orders linked to valid customers.
    orders_clean = (
        orders_df
        .join(customers_df,on="customer_id",how="left_semi")           
    )
    
    rejected_order_customer_count = order_customer_rej.count()
    orders_clean_count = orders_clean.count()
    
    if orders_count!=rejected_order_customer_count+orders_clean_count:
        raise ValueError(f"Order - Customer relationship reconciliation failed.\nOrders Count: {orders_count}, Rejected Count: {rejected_order_customer_count}, Valid Count: {orders_clean_count}")
    
    # Order items whose order is missing or whose order was
    # rejected because its customer was missing.
    order_items_orders_rej = (
        order_items_df
        .join(orders_clean,"order_id",how="left_anti")
        .withColumn("_relationship_error",F.lit("VALID_ORDER_NOT_FOUND"))
        .withColumn("_rejected_timestamp",F.current_timestamp())
    )
    
    # Product validation only for items linked to valid orders
    order_items_orders_clean = (
        order_items_df
        .join(orders_clean, "order_id","left_semi")
    )
    
    # Items connected to a valid order but missing a product.
    order_items_products_rej = (
        order_items_orders_clean
        .join(products_df,"product_id",how="left_anti")
        .withColumn("_relationship_error",F.lit("PRODUCT_NOT_FOUND"))
        .withColumn("_rejected_timestamp",F.current_timestamp())
    )
    
    # Final order-item set with both relationships valid.
    order_items_clean = (
        order_items_orders_clean
        .join(products_df,"product_id","left_semi")
    )
    
    rejected_order_item_order_count = order_items_orders_rej.count()
    rejected_order_item_product_count = order_items_products_rej.count()
    order_items_clean_count = order_items_clean.count()
    
    if order_items_count!=rejected_order_item_order_count+rejected_order_item_product_count+order_items_clean_count:
        raise ValueError(f"Order-item relationship reconciliation.\nOrder items Count: {order_items_count}, Rejected Order item Count: {rejected_order_item_order_count}, Rejected Product Count: {rejected_order_item_product_count}, Valid count: {order_items_clean_count}")
    
    print(f"Orders rejected due to missing customer: {rejected_order_customer_count}")
    print(f"Order items rejected due to missing valid order: {rejected_order_item_order_count}")
    print(f"Order items rejected due to missing product: {rejected_order_item_product_count}")
    
    # Enriched fact table
    
    orders_alias=orders_clean.alias("orders")
    order_items_alias=order_items_clean.alias("order_items")
    customers_alias=customers_df.alias("customers")
    products_alias=products_df.alias("products")
    
    enriched_df = (
        order_items_alias
            .join(orders_alias, on=F.col("order_items.order_id")==F.col("orders.order_id"), how="inner")
            .join(customers_alias, on=F.col("orders.customer_id")==F.col("customers.customer_id"), how="inner")
            .join(products_alias, on=F.col("order_items.product_id")==F.col("products.product_id"), how="inner")
            .select(
                F.col("order_items.order_item_id").alias("order_item_id"),
                F.col("orders.order_id").alias("order_id"),
                F.col("orders.order_date").alias("order_date"),
                F.col("orders.customer_id").alias("customer_id"),
                F.concat_ws(" ",F.col("customers.first_name"),F.col("customers.last_name"),).alias("customer_name"),
                F.col("customers.state").alias("customer_state"),
                F.col("customers.country").alias("customer_country"),
                F.col("order_items.product_id").alias("product_id"),
                F.col("products.product_name").alias("product_name"),
                F.col("products.category").alias("category"),
                F.col("products.brand").alias("brand"),
                F.col("order_items.quantity").alias("quantity"),
                F.col("order_items.unit_price").alias("unit_price"),
                F.col("order_items.discount_amount").alias("discount_amount"),
                F.col("order_items.line_total").alias("line_total"),
                F.col("orders.order_status").alias("order_status"),
            )
            .withColumn("gross_line_amount",F.round(F.col("quantity")* F.col("unit_price"),2,))
            .withColumn("net_line_amount",F.round((F.col("quantity")* F.col("unit_price")) - F.coalesce(F.col("discount_amount"),F.lit(0),),2,),)
            .withColumn("order_year",F.year(F.col("order_date")),)
            .withColumn("order_month",F.month(F.col("order_date")),)
            .withColumn("order_date_key",F.date_format(F.col("order_date"),"yyyyMMdd",).cast("int"),)
            .withColumn("_silver_processed_timestamp",F.current_timestamp(),)
    )
    
    enriched_count = enriched_df.count()
    
    if enriched_count != order_items_clean_count:
        raise ValueError(f"Enriched-sales row-count validation failed:\n valid_order_items={order_items_clean_count}, enriched={enriched_count}")
    
    enriched_key_stats = (
        enriched_df
        .agg(
            F.count("*").alias("row_count"),
            F.countDistinct(
                "order_item_id"
            ).alias("distinct_order_item_count"),
        )
        .first()
    )

    if (
        enriched_key_stats["row_count"]
        != enriched_key_stats[
            "distinct_order_item_count"
        ]
    ):
        raise ValueError(
            "Join multiplication detected: "
            "order_item_id is no longer unique."
        )
        
    
    # Write rejected files:
    order_customer_rej.write.mode("overwrite").parquet(SILVER_REJECT_BASE_PATH+"/order_customer_relationship/")
    order_items_orders_rej.write.mode("overwrite").parquet(SILVER_REJECT_BASE_PATH+"/order_items_orders_relationship/")
    order_items_products_rej.write.mode("overwrite").parquet(SILVER_REJECT_BASE_PATH+"/order_items_products_relationship/")
    
    # Write enriched file:
    ENRICHED_SALES_PATH = (
    SILVER_INPUT_BASE_PATH
    + "sales_order_items_enriched/"
)
    enriched_df.write.mode("overwrite").parquet(ENRICHED_SALES_PATH)
    
    print(
        f"Enriched sales written to: "
        f"{ENRICHED_SALES_PATH}"
    )

    print(
        f"Final enriched row count: "
        f"{enriched_count}"
    )
    
    
    print("Sample enriched sales records:")

    (
        enriched_df
        .select(
            "order_item_id",
            "order_id",
            "order_date",
            "customer_name",
            "customer_state",
            "product_name",
            "category",
            "quantity",
            "gross_line_amount",
            "discount_amount",
            "net_line_amount",
        )
        .show(20, truncate=False)
    )
    
    print(
        "Silver enriched-sales processing "
        "completed successfully."
    )


if __name__ == "__main__":
    main()