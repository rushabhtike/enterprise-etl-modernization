from urllib.parse import quote_plus

from sqlalchemy import create_engine

from common.config import Config


connection_string = quote_plus(
    f"DRIVER={{{Config.DB_DRIVER}}};"
    f"SERVER={Config.DB_SERVER},{Config.DB_PORT};"
    f"DATABASE={Config.DB_DATABASE};"
    f"UID={Config.DB_USERNAME};"
    f"PWD={Config.DB_PASSWORD};"
    "TrustServerCertificate=yes;"
)

engine = create_engine(
    f"mssql+pyodbc:///?odbc_connect={connection_string}",
    fast_executemany=True,
    pool_pre_ping=True,
)