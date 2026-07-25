from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

BRONZE_INPUT_BASE_PATH=(
    "/home/hadoop/workspace/docker/glue/output/bronze/customers/"
)

SILVER_OUTPUT_BASE_PATH=(
    "/home/hadoop/workspace/docker/glue/output/silver/customers/"
)

REJECT_BASE_PATH=(
    "/home/hadoop/workspace/docker/glue/output/reject/customers/"
)

EMAIL_PATTERN = (
    r"^[A-Za-z0-9._%+-]+@"
    r"[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
)


def clean_customers(customers: DataFrame)->DataFrame:
    cleaned_customers=(
        customers
        .withColumn("first_name",F.trim(F.initcap(F.col("first_name"))))
        .withColumn("last_name",F.trim(F.initcap(F.col("last_name"))))
        .withColumn("email",F.lower(F.trim(F.col("email"))))
        .withColumn("phone",F.regexp_replace(F.trim(F.col("phone")),r"[^0-9]",""))
        .withColumn("gender", 
                    F.when(F.lower(F.trim(F.col("gender")))=="m",F.lit("MALE"))
                    .when(F.lower(F.trim(F.col("gender")))=="f",F.lit("FEMALE"))
                    .when(F.lower(F.trim(F.col("gender")))=="o",F.lit("OTHER"))
                    .otherwise(F.upper(F.trim(F.col("gender"))))
                )
        .withColumn("city",F.trim(F.initcap(F.col("city"))))
        .withColumn("state",F.trim(F.initcap(F.col("state"))))
        .withColumn("country",F.trim(F.initcap(F.col("country"))))
        .withColumn("source_system",F.trim(F.initcap(F.col("source_system"))))
    )

    return cleaned_customers

def add_data_quality_errors(customers: DataFrame)->DataFrame:
    df=(
    customers.withColumn("dq_errors",
                  F.array_compact(
                        F.array(
                            F.when(F.col("customer_id").isNull(), F.lit("CUSTOMER_ID_MISSING")),
                            F.when((F.col("first_name").isNull()) | (F.col("first_name") == ""), F.lit("FNAME_MISSING")),
                            F.when((F.col("last_name").isNull()) | (F.col("last_name") == ""), F.lit("LNAME_MISSING")),
                            F.when(F.col("email").isNotNull() & (F.col("email") != "") & ~F.col("email").rlike(EMAIL_PATTERN),F.lit("INVALID_EMAIL")),
                            F.when((F.col("phone").isNotNull() & (F.col("phone") != "") & ((F.length(F.col("phone")) < 10) | (F.length(F.col("phone")) > 15))) | (F.col("phone").isNull()) | (F.col("phone") == ""),F.lit("INVALID_PHONE")),
                            F.when((F.col("date_of_birth").isNull()) | (F.col("date_of_birth") == ""), F.lit("date_of_birth_MISSING")),
                            F.when((F.col("gender").isNull()) | (F.col("gender") == ""), F.lit("gender_MISSING")),
                            F.when((F.col("city").isNull()) | (F.col("city") == ""), F.lit("city_MISSING")),
                            F.when((F.col("state").isNull()) | (F.col("state") == ""), F.lit("state_MISSING")),
                            F.when((F.col("country").isNull()) | (F.col("country") == ""), F.lit("country_MISSING")),
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

    return df

def main()->None:
    spark_context=SparkContext.getOrCreate()
    glue_context=GlueContext(spark_context)
    spark=glue_context.spark_session

    spark.sparkContext.setLogLevel("WARN")

    print(f"Reading customer data from: {BRONZE_INPUT_BASE_PATH}")

    bronze_customers=spark.read.parquet(BRONZE_INPUT_BASE_PATH)

    bronze_customers.printSchema()
    source_count = bronze_customers.count()
    print(f"Bronze customer count: {source_count}")

    validated_customers=(
        bronze_customers
        .transform(clean_customers)
        .transform(add_data_quality_errors)
        .cache()
    )

    valid_customers=(
        validated_customers
        .filter(F.size(F.col("dq_errors"))==0)
        .drop("dq_errors")
        .withColumn("_silver_processed_timestamp",F.current_timestamp())
    )

    rejected_customers=(
        validated_customers
        .filter(F.size(F.col("dq_errors"))>0)
        .withColumn("_rejected_timestamp",F.current_timestamp())
    )
    
    valid_customers_count=valid_customers.count()
    rejected_customers_count=rejected_customers.count()
    print(f"Valid customers count is: {valid_customers_count}")
    print(f"Rejected customers count is: {rejected_customers_count}")
    
    if source_count!=valid_customers_count+rejected_customers_count:
        raise ValueError(
            "Customer reconciliation failed: "
            f"source={source_count}"
            f"valid={valid_customers_count}"
            f"reject={rejected_customers_count}"
        )
    
    valid_customers.write.mode("overwrite").parquet(
        SILVER_OUTPUT_BASE_PATH
    )

    rejected_customers.write.mode("overwrite").parquet(
        REJECT_BASE_PATH
    )

    print(
        "Silver customers written to:"
        f"{SILVER_OUTPUT_BASE_PATH}"
    )

    print(
        "Rejected customers written to:"
        f"{REJECT_BASE_PATH}"
    )

    print("Data quality error summary:")
    (
        rejected_customers
        .select(
            F.explode("dq_errors").alias("dq_error")
        )
        .groupBy("dq_error")
        .count()
        .orderBy(F.desc("count"))
        .show(truncate=False)
    )

    print("Sample valid customers:")
    valid_customers.show(10,truncate=False)

    print(
        "Silver customer processing completed "
        "successfully."
    )

    validated_customers.unpersist()

if __name__=="__main__":
    main()