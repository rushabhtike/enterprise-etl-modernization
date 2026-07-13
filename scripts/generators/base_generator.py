from abc import ABC, abstractmethod
import pandas as pd

from common.logger import get_logger
from loaders.sql_server_loader import SQLServerLoader


class BaseGenerator(ABC):

    def __init__(self):
        self.logger = get_logger(self.__class__.__name__)
        self.loader = SQLServerLoader()

    @property
    @abstractmethod
    def schema(self) -> str:
        pass

    @property
    @abstractmethod
    def table(self) -> str:
        pass

    @abstractmethod
    def generate(self) -> pd.DataFrame:
        pass

    def validate(self, df: pd.DataFrame):

        if df.empty:
            raise ValueError("Generated DataFrame is empty.")

        self.logger.info(
            "Validation successful. %s rows generated.",
            len(df)
        )

    def run(self):

        self.logger.info(
            "Generating %s.%s",
            self.schema,
            self.table
        )

        df = self.generate()

        self.validate(df)

        self.loader.load(
            dataframe=df,
            schema=self.schema,
            table=self.table
        )

        self.logger.info("Finished loading.")