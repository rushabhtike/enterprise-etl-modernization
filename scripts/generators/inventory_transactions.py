import random

import pandas as pd
from faker import Faker
from sqlalchemy import text

from common.config import Config
from common.database import engine
from generators.base_generator import BaseGenerator


fake = Faker("en_IN")
random.seed(Config.RANDOM_SEED)
Faker.seed(Config.RANDOM_SEED)


class InventoryTransactionsGenerator(BaseGenerator):

    @property
    def schema(self) -> str:
        return "inventory"

    @property
    def table(self) -> str:
        return "inventory_transactions"

    def generate(self) -> pd.DataFrame:
        with engine.connect() as connection:
            inventory_pairs = connection.execute(
                text(
                    """
                    SELECT store_id, product_id
                    FROM inventory.inventory
                    WHERE is_deleted = 0
                    """
                )
            ).fetchall()

            order_ids = [
                row[0]
                for row in connection.execute(
                    text(
                        """
                        SELECT order_id
                        FROM sales.orders
                        WHERE is_deleted = 0
                        """
                    )
                )
            ]

        if not inventory_pairs:
            raise ValueError(
                "Inventory must be loaded before inventory transactions."
            )

        transaction_count = getattr(
            Config,
            "NUM_INVENTORY_TRANSACTIONS",
            min(50000, len(inventory_pairs) * 4),
        )

        transaction_types = [
            "SALE",
            "PURCHASE",
            "RETURN",
            "ADJUSTMENT",
        ]

        transaction_weights = [0.55, 0.25, 0.10, 0.10]

        rows = []

        for _ in range(transaction_count):
            store_id, product_id = random.choice(inventory_pairs)

            transaction_type = random.choices(
                transaction_types,
                weights=transaction_weights,
                k=1,
            )[0]

            reference_id = None

            if transaction_type == "SALE" and order_ids:
                reference_id = random.choice(order_ids)

            rows.append(
                {
                    "store_id": store_id,
                    "product_id": product_id,
                    "transaction_type": transaction_type,
                    "quantity": random.randint(1, 20),
                    "transaction_timestamp": (
                        fake.date_time_between(
                            start_date="-2y",
                            end_date="now",
                        )
                    ),
                    "reference_id": reference_id,
                    "created_timestamp": pd.Timestamp.now(
                        tz="UTC"
                    ).tz_localize(None),
                    "updated_timestamp": pd.Timestamp.now(
                        tz="UTC"
                    ).tz_localize(None),
                    "source_system": "SQLSERVER",
                    "is_deleted": False,
                }
            )

        return pd.DataFrame(rows)