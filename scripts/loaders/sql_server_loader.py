import pandas as pd

from common.database import engine
from common.logger import get_logger

logger = get_logger(__name__)


class SQLServerLoader:
    """Load pandas DataFrames into SQL Server."""

    def load(
        self,
        dataframe: pd.DataFrame,
        schema: str,
        table: str,
        if_exists: str = "append",
        chunk_size: int = 1000,
    ) -> None:
        if dataframe.empty:
            raise ValueError(f"Cannot load an empty DataFrame into {schema}.{table}.")

        logger.info(
            "Loading %s rows into %s.%s",
            len(dataframe),
            schema,
            table,
        )

        dataframe.to_sql(
            name=table,
            schema=schema,
            con=engine,
            if_exists=if_exists,
            index=False,
            chunksize=chunk_size,
        )

        logger.info(
            "Successfully loaded %s rows into %s.%s",
            len(dataframe),
            schema,
            table,
        )