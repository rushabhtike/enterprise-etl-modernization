import random

import pandas as pd
from sqlalchemy import text

from common.database import engine
from generators.base_generator import BaseGenerator


random.seed(42)


class OrderItemsGenerator(BaseGenerator):

    @property
    def schema(self) -> str:
        return "sales"

    @property
    def table(self) -> str:
        return "order_items"

    def generate(self) -> pd.DataFrame:
        with engine.connect() as connection:
            orders = connection.execute(
                text(
                    """
                    SELECT order_id, order_date, order_status
                    FROM sales.orders
                    WHERE is_deleted = 0
                    """
                )
            ).fetchall()

            products = connection.execute(
                text(
                    """
                    SELECT product_id, unit_price
                    FROM master_data.products
                    WHERE is_deleted = 0
                      AND is_active = 1
                    """
                )
            ).fetchall()

        if not orders or not products:
            raise ValueError(
                "Orders and products must be loaded before order items."
            )

        rows = []

        for order_id, order_date, order_status in orders:
            selected_products = random.sample(
                products,
                k=min(
                    random.randint(1, 5),
                    len(products),
                ),
            )

            for product_id, unit_price in selected_products:
                quantity = random.randint(1, 4)

                discount_rate = random.choices(
                    [0, 0.05, 0.10, 0.15],
                    weights=[0.65, 0.20, 0.10, 0.05],
                    k=1,
                )[0]

                gross_amount = float(unit_price) * quantity
                discount_amount = round(
                    gross_amount * discount_rate,
                    2,
                )

                line_total = round(
                    gross_amount - discount_amount,
                    2,
                )

                if order_status == "CANCELLED":
                    line_total = 0.00

                rows.append(
                    {
                        "order_id": order_id,
                        "product_id": product_id,
                        "quantity": quantity,
                        "unit_price": float(unit_price),
                        "discount_amount": discount_amount,
                        "line_total": line_total,
                        "created_timestamp": order_date,
                        "updated_timestamp": order_date,
                        "source_system": "SQLSERVER",
                        "is_deleted": False,
                    }
                )

        return pd.DataFrame(rows)