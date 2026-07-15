from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import functions as F
import argparse
import json

from common.jdbc import read_sql_server_table

BRONZE_OUTPUT_BASE_PATH=(
    "/home/hadoop/workspace/docker/glue/output/bronze"
)

CONFIG_PATH=(
    "/home/hadoop/workspace/docker/glue/config/bronze_tables.json"
)


def load_table_config(target_name:str)->dict:
    with open(CONFIG_PATH,'r',encoding='utf-8') as file:
        config=json.load(file)

        for table in config["tables"]:
            if table["target_name"]==target_name:
                return table
        
        raise ValueError(
            f"No Bronze config found for: {target_name}"
        )

def parse_arguments()->argparse.Namespace:
    parser = argparse.ArgumentParser(description='Ingest a SQL Server table into Bronze Parquet')
    parser.add_argument("--target-name",required=True)
    return parser.parse_args()


def main()->None:

    spark_context=SparkContext.getOrCreate()
    glue_context=GlueContext(spark_context)
    spark=glue_context.spark_session

    spark.sparkContext.setLogLevel("WARN")

    args = parse_arguments()
    table_config=load_table_config(args.target_name)
    source_table=table_config["source_table"]
    target_name=table_config["target_name"]

    print(source_table, target_name)

    base_df=read_sql_server_table(spark=spark,table_name=source_table)

    bronze_df=(
        base_df
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("source_table",F.lit("sales.orders"))
        .withColumn("source_system",F.lit("SQLSERVER"))
    )

    print(f"Source {source_table} Bronze table orders schema:")
    bronze_df.printSchema()

    source_count = bronze_df.count()
    print(f"Source {source_table} count: {source_count}")

    BRONZE_OUTPUT_PATH=str(BRONZE_OUTPUT_BASE_PATH+"/"+target_name)

    print(BRONZE_OUTPUT_PATH)

    bronze_df.write.mode("overwrite").parquet(BRONZE_OUTPUT_PATH)

    written_orders = spark.read.parquet(
        BRONZE_OUTPUT_PATH
    )

    target_count=written_orders.count()
    print(f"Target {target_name} Written file count: {target_count}")

    if source_count != target_count:
        raise ValueError(
            f"{source_table} Bronze row-count validation failed: "
            f"source={source_count}, target={target_count}"
        )

    print(f"{source_table} Bronze row-count validation successful.")

    written_orders.show(5, truncate=False)


if __name__ == "__main__":
    main()



