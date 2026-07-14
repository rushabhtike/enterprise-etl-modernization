from sqlalchemy import text
from common.database import engine
from generators.customers import CustomersGenerator
from generators.employees import EmployeesGenerator
from generators.products import ProductsGenerator
from generators.stores import StoresGenerator
from generators.suppliers import SuppliersGenerator
from generators.inventory import InventoryGenerator
from generators.inventory_transactions import InventoryTransactionsGenerator
from generators.order_items import OrderItemsGenerator
from generators.orders import OrdersGenerator


def generate_and_load(generator) -> None:
    dataframe = generator.generate()
    generator.load(dataframe)


def update_order_totals() -> None:
    statement = text(
        """
        UPDATE orders
        SET order_total = totals.calculated_total,
            updated_timestamp = SYSUTCDATETIME()
        FROM sales.orders AS orders
        INNER JOIN
        (
            SELECT
                order_id,
                SUM(line_total) AS calculated_total
            FROM sales.order_items
            WHERE is_deleted = 0
            GROUP BY order_id
        ) AS totals
            ON orders.order_id = totals.order_id
        """
    )

    with engine.begin() as connection:
        connection.execute(statement)


def main() -> None:
    generators = [
        SuppliersGenerator(),
        StoresGenerator(),
        ProductsGenerator(),
        CustomersGenerator(),
        EmployeesGenerator(),
        OrdersGenerator(),
        OrderItemsGenerator(),
        InventoryGenerator(),
        InventoryTransactionsGenerator(),
    ]

    for generator in generators:
        print(
            f"Generating and loading "
            f"{generator.schema}.{generator.table}..."
        )

        generate_and_load(generator)

    update_order_totals()

    print("Data generation completed successfully.")


if __name__ == "__main__":
    main()