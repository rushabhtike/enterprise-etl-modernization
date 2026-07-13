from sqlalchemy import text

from common.database import engine
from common.logger import get_logger

logger = get_logger(__name__)

class SQLServerLoader:
    def load(
            self,
            dataframe,
            schema:str,
            table:str,
            if_exists: str = "append"
    ) -> None:
        
        logger.info(
            "Loading %s rows into %s.%s",
            len(dataframe),
            schema,
            table
        )

        dataframe.to_sql(
            name=table,
            schema=schema,
            con=engine,
            if_exists=if_exists,
            index=False,
            method="multi",
            chunksize=5000,
        )

        logger.info("Load completed.")