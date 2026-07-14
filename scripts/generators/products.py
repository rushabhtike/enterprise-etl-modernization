import random

import pandas as pd
from sqlalchemy import text

from common.config import Config
from common.database import engine
from common.reference_data import PRODUCT_BRANDS, PRODUCT_CATEGORIES
from generators.base_generator import BaseGenerator


random.seed(Config.RANDOM_SEED)


class ProductsGenerator(BaseGenerator):

    @property
    def schema(self) -> str:
        return "master_data"

    @property
    def table(self) -> str:
        return "products"

    def generate(self) -> pd.DataFrame:
        with engine.connect() as connection:
            supplier_ids = [
                row[0]
                for row in connection.execute(
                    text(
                        """
                        SELECT supplier_id
                        FROM master_data.suppliers
                        WHERE is_deleted = 0
                        """
                    )
                )
            ]

        if not supplier_ids:
            raise ValueError("Suppliers must be loaded before products.")

        subcategories = {
            "Laptops": ["Business", "Gaming", "Ultrabook"],
            "Desktops": ["Business", "Gaming", "All-in-One"],
            "Monitors": ["Office", "Gaming", "Professional"],
            "Keyboards": ["Mechanical", "Wireless", "Standard"],
            "Mouse": ["Gaming", "Wireless", "Standard"],
            "Printers": ["Laser", "Inkjet", "Multifunction"],
            "Networking": ["Router", "Switch", "Access Point"],
            "Storage": ["SSD", "HDD", "External Drive"],
            "Mobile Phones": ["Budget", "Midrange", "Premium"],
            "Tablets": ["Wi-Fi", "Cellular", "Professional"],
            "Televisions": ["LED", "OLED", "QLED"],
            "Audio": ["Headphones", "Speakers", "Soundbar"],
            "Gaming": ["Console", "Controller", "Accessory"],
            "Accessories": ["Cable", "Adapter", "Charger"],
        }

        rows = []

        for product_number in range(1, Config.NUM_PRODUCTS + 1):
            category = random.choice(PRODUCT_CATEGORIES)
            brand = random.choice(PRODUCT_BRANDS)
            subcategory = random.choice(
                subcategories.get(category, ["General"])
            )

            cost_price = round(random.uniform(250, 75000), 2)
            margin = random.uniform(1.10, 1.45)
            unit_price = round(cost_price * margin, 2)

            rows.append(
                {
                    "supplier_id": random.choice(supplier_ids),
                    "product_name": (
                        f"{brand} {subcategory} "
                        f"{category} Model {product_number:05d}"
                    ),
                    "category": category,
                    "subcategory": subcategory,
                    "brand": brand,
                    "unit_price": unit_price,
                    "cost_price": cost_price,
                    "is_active": random.random() >= 0.02,
                    "created_timestamp": pd.Timestamp.now(
                        tz="UTC"
                    ).tz_localize(None)
                    - pd.Timedelta(days=random.randint(180, 900)),
                    "updated_timestamp": pd.Timestamp.now(
                        tz="UTC"
                    ).tz_localize(None)
                    - pd.Timedelta(days=random.randint(0, 120)),
                    "source_system": "SQLSERVER",
                    "is_deleted": random.random() < 0.005,
                }
            )

        return pd.DataFrame(rows)