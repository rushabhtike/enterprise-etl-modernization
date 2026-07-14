from abc import ABC, abstractmethod

import pandas as pd

from loaders.sql_server_loader import SQLServerLoader


class BaseGenerator(ABC):
    """Base class for all data generators."""

    def __init__(self):
        self.loader = SQLServerLoader()

    @property
    @abstractmethod
    def schema(self) -> str:
        """Target SQL Server schema."""
        pass

    @property
    @abstractmethod
    def table(self) -> str:
        """Target SQL Server table."""
        pass

    @abstractmethod
    def generate(self) -> pd.DataFrame:
        """Generate source data."""
        pass

    def validate(self, dataframe: pd.DataFrame) -> None:
        """Generic validation."""

        if dataframe.empty:
            raise ValueError(
                f"{self.schema}.{self.table} generated an empty DataFrame."
            )

        if dataframe.columns.duplicated().any():
            raise ValueError(
                f"{self.schema}.{self.table} contains duplicate column names."
            )

    def load(self, dataframe: pd.DataFrame) -> None:
        """Validate and load the generated data."""

        self.validate(dataframe)

        self.loader.load(
            dataframe=dataframe,
            schema=self.schema,
            table=self.table,
        )