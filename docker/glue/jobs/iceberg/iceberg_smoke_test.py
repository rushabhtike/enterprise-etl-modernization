from awsglue.context import GlueContext
from pyspark.context import SparkContext


def main() -> None:
    spark_context = SparkContext.getOrCreate()
    glue_context = GlueContext(spark_context)
    spark = glue_context.spark_session

    spark.sparkContext.setLogLevel("WARN")

    spark.sql(
        "CREATE NAMESPACE IF NOT EXISTS local.gold"
    )
    
    spark.sql(
        "CREATE NAMESPACE IF NOT EXISTS local.silver"
    )
    
    spark.sql(
        "CREATE NAMESPACE IF NOT EXISTS local.bronze"
    )
    
    print("Available Spark catalogs:")
    spark.sql("SHOW CATALOGS").show(truncate=False)

    print("Namespaces in local catalog:")
    spark.sql(
        "SHOW NAMESPACES IN local"
    ).show(truncate=False)

    print("Local Iceberg catalog configured successfully.")


if __name__ == "__main__":
    main()