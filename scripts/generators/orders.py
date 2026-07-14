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


class OrdersGenerator(BaseGenerator):

    @property
    def schema(self) -> str:
        return "sales"

    @property
    def table(self) -> str:
        return "orders"

    def generate(self) -> pd.DataFrame:
        with engine.connect() as connection:
            customer_ids = [
                row[0]
                for row in connection.execute(
                    text(
                        """
                        SELECT customer_id
                        FROM master_data.customers
                        WHERE is_deleted = 0
                        """
                    )
                )
            ]

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

        if not customer_ids or not store_ids:
            raise ValueError(
                "Customers and stores must be loaded before orders."
            )

        statuses = [
            "PENDING",
            "PROCESSING",
            "SHIPPED",
            "DELIVERED",
            "CANCELLED",
        ]

        status_weights = [0.03, 0.05, 0.08, 0.81, 0.03]

        payment_methods = [
            "CASH",
            "CREDIT_CARD",
            "DEBIT_CARD",
            "UPI",
            "NET_BANKING",
        ]

        rows = []

        for _ in range(Config.NUM_ORDERS):
            order_date = fake.date_time_between(
                start_date="-2y",
                end_date="now",
            )

            rows.append(
                {
                    "customer_id": random.choice(customer_ids),
                    "store_id": random.choice(store_ids),
                    "order_date": order_date,
                    "order_status": random.choices(
                        statuses,
                        weights=status_weights,
                        k=1,
                    )[0],
                    "payment_method": random.choice(payment_methods),
                    "order_total": 0.00,
                    "created_timestamp": order_date,
                    "updated_timestamp": order_date
                    + pd.Timedelta(
                        hours=random.randint(0, 72)
                    ),
                    "source_system": "SQLSERVER",
                    "is_deleted": False,
                }
            )

        return pd.DataFrame(rows)