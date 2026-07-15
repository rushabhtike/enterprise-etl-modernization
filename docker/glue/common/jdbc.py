import os
from pyspark.sql import DataFrame, SparkSession

SQL_SERVER_DRIVER="com.microsoft.sqlserver.jdbc.SQLServerDriver"

def require_environment_variables(name:str)->str:
    """Return a required environment variable or raise a clear error."""
    value=os.getenv(name)

    if not value:
        raise ValueError(f"Missing reuqired environment variable: {name}")
    
    return value

def get_jdbc_url()->str:
    """Build the SQL Server JDBC connection URL."""
    host=os.getenv("JDBC_HOST","sqlserver")
    port=os.getenv("JDBC_PORT","1433")
    database=os.getenv("JDBC_DATABASE","retail_db")

    return (
        f"jdbc:sqlserver://{host}:{port};"
        f"databaseName={database};"
        "encrypt=true;"
        "trustServerCertificate=true;"
    )

def read_sql_server_table(spark: SparkSession, table_name: str)->DataFrame:
    """Read a complete SQL Server table into a Spark DataFrame."""
    username=os.getenv("JDBC_USERNAME","sa")
    password=require_environment_variables("MSSQL_SA_PASSWORD")

    return (
        spark.read.format("jdbc")
        .option("url",get_jdbc_url())
        .option("dbtable",table_name)
        .option("user",username)
        .option("password",password)
        .option("driver",SQL_SERVER_DRIVER)
        .option("fetchsize","10000")
        .load()
    )