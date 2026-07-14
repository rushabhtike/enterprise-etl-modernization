import random

import pandas as pd
from sqlalchemy import text

from common.database import engine
from generators.base_generator import BaseGenerator


random.seed(42)


class InventoryGenerator(BaseGenerator):

    @property
    def schema(self) -> str:
        return "inventory"

    @property
    def table(self) -> str:
        return "inventory"

    def generate(self) -> pd.DataFrame:
        with engine.connect() as connection:
            store_ids = [
                row[0]
                for row in connection.execute(
                    text(
                        """
                        SELECT store_id
                        FROM master_data.stores
                        WHERE is_deleted = 0
                        """
                    )
                )
            ]

            product_ids = [
                row[0]
                for row in connection.execute(
                    text(
                        """
                        SELECT product_id
                        FROM master_data.products
                        WHERE is_deleted = 0
                          AND is_active = 1
                        """
                    )
                )
            ]

        if not store_ids or not product_ids:
            raise ValueError(
                "Stores and products must be loaded before inventory."
            )

        rows = []
        current_timestamp = pd.Timestamp.now(
            tz="UTC"
        ).tz_localize(None)

        for store_id in store_ids:
            for product_id in product_ids:
                reorder_level = random.randint(5, 25)
                quantity_on_hand = random.randint(
                    0,
                    reorder_level * 8,
                )

                rows.append(
                    {
                        "store_id": store_id,
                        "product_id": product_id,
                        "quantity_on_hand": quantity_on_hand,
                        "reorder_level": reorder_level,
                        "last_updated": current_timestamp,
                        "created_timestamp": current_timestamp,
                        "updated_timestamp": current_timestamp,
                        "source_system": "SQLSERVER",
                        "is_deleted": False,
                    }
                )

        return pd.DataFrame(rows)