import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator

with DAG(
    dag_id="test_glue_connection",
    description="verfiy that airflwo can execute commands in glue-dev",
    start_date=datetime.datetime(2026,8,1),
    schedule=None,
    catchup=False,
    tags=["local","glue","test"],   
) as dag:

    test_glue_container=BashOperator(
        task_id="test_glue_container",
        bash_command="""
            docker exec glue-dev bash -lc '
                echo "Airflow successfully reached glue-dev"
                echo "Container user: $(whoami)"
                echo "Working directory: $(pwd)"
                spark-submit --version
            '
        """
    )