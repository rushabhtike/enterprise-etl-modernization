from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import functions as F

def main() -> None:
    spark_context=SparkContext.getOrCreate()
    glue_context=GlueContext(spark_context)
    spark=glue_context.spark_session

    source_data=[
        (1,"orders"),
        (2,"customers"),
        (3,"products")
    ]

    df=spark.createDataFrame(source_data, ["table_id","table_name"])

    result=df.withColumn("table_upper_name", F.upper(F.col("table_name")))

    result.show(truncate=False)

    print(f"Spark Version:{spark.version}")

if __name__=="__main__":
    main()