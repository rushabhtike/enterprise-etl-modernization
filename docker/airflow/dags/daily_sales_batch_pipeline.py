from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator

GLUE_WORKSPACE="/home/hadoop/workspace"

ICEBERG_WRAPPER=(
    f"{GLUE_WORKSPACE}/docker/glue/scripts/"
    "run_iceberg_job.sh"
)

BRONZE_JOB=(
    f"{GLUE_WORKSPACE}/docker/glue/jobs/"
    "/bronze/bronze_ingest.py"
)

SILVER_CUSTOMERS_JOB=(
    f"{GLUE_WORKSPACE}/docker/glue/jobs/"
    "/silver/silver_customers.py"
)

SILVER_PRODUCTS_JOB=(
    f"{GLUE_WORKSPACE}/docker/glue/jobs/"
    "/silver/silver_products.py"
)

SILVER_ORDERS_JOB=(
    f"{GLUE_WORKSPACE}/docker/glue/jobs/"
    "/silver/silver_orders.py"
)

SILVER_ORDER_ITEMS_JOB=(
    f"{GLUE_WORKSPACE}/docker/glue/jobs/"
    "/silver/silver_order_items.py"
)

SILVER_SALES_ENRICHED_JOB=(
    f"{GLUE_WORKSPACE}/docker/glue/jobs/"
    "/silver/silver_sales_enriched.py"
)

GOLD_DAILY_SALES_SUMMARY_JOB=(
    f"{GLUE_WORKSPACE}/docker/glue/jobs/"
    "/gold/gold_daily_sales_summary.py"
)

def build_glue_command(
    job_path: str,
    job_arguments: str = "",
) -> str:
    return (
        "docker exec glue-dev bash -lc '"
        f"cd {GLUE_WORKSPACE} && "
        f"{ICEBERG_WRAPPER} {job_path} {job_arguments}"
        "'"
    )

BATCH_ARGUMENTS = (
    '--batch-id "{{ dag_run.conf.get(\'batch_id\', dag_run.run_id) }}" '
    '--batch-date "{{ dag_run.conf.get(\'batch_date\', ds) }}"'
)

default_args={
    "owner":"rushabh",
    "retries":1,
    "retry_delay":timedelta(minutes=2)
}

with DAG(
    dag_id="daily_sales_batch_pipeline",
    description="Daily SQL Server to Bronze, Silver and Gold. Batch pipeline ",
    start_date=datetime(2026,8,1, tzinfo=ZoneInfo("Asia/Kolkata")),
    schedule=None,
    catchup=None,
    default_args=default_args,
    max_active_runs=1,
    max_active_tasks=1,
    tags=["daily-batch","pyspark","iceberg"]
) as dag:
    
    bronze_customers=BashOperator(
        task_id="bronze_customers",
        bash_command=build_glue_command(
            BRONZE_JOB, "--target-name customers"
        )
    )
    
    bronze_products=BashOperator(
        task_id="bronze_products",
        bash_command=build_glue_command(
            BRONZE_JOB, "--target-name products"
        )
    )
    
    bronze_orders=BashOperator(
            task_id="bronze_orders",
            bash_command=build_glue_command(
                BRONZE_JOB, "--target-name orders"
            )
        )
    
    bronze_order_items=BashOperator(
            task_id="bronze_order_items",
            bash_command=build_glue_command(
                BRONZE_JOB, "--target-name order_items"
            )
        )
    
    silver_customers=BashOperator(
                task_id="silver_customers",
                bash_command=build_glue_command(
                    SILVER_CUSTOMERS_JOB
                )
            )
    
    silver_products=BashOperator(
                    task_id="silver_products",
                    bash_command=build_glue_command(
                        SILVER_PRODUCTS_JOB
                    )
                )
    
    silver_orders=BashOperator(
                    task_id="silver_orders",
                    bash_command=build_glue_command(
                        SILVER_ORDERS_JOB
                    )
                )
    
    silver_order_items=BashOperator(
                    task_id="silver_order_items",
                    bash_command=build_glue_command(
                        SILVER_ORDER_ITEMS_JOB
                    )
                )
    
    silver_sales_enriched=BashOperator(
                        task_id="silver_sales_enriched",
                        bash_command=build_glue_command(
                            SILVER_SALES_ENRICHED_JOB, BATCH_ARGUMENTS
                        )
                    )
    
    gold_daily_sales_summary=BashOperator(
                        task_id="gold_daily_sales_summary",
                        bash_command=build_glue_command(
                            GOLD_DAILY_SALES_SUMMARY_JOB, BATCH_ARGUMENTS
                        )
                    )
    
    bronze_customers>>silver_customers
    bronze_orders>>silver_orders
    bronze_products>>silver_products
    bronze_order_items>>silver_order_items
    
    [
    silver_customers,
    silver_products,
    silver_orders,
    silver_order_items
    ] >> silver_sales_enriched
    
    silver_sales_enriched>>gold_daily_sales_summary

