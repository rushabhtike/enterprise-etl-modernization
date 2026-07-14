import random

import pandas as pd
from faker import Faker

from common.config import Config
from common.reference_data import INDIAN_CITIES
from generators.base_generator import BaseGenerator


fake = Faker("en_IN")
random.seed(Config.RANDOM_SEED)
Faker.seed(Config.RANDOM_SEED)


class StoresGenerator(BaseGenerator):

    @property
    def schema(self) -> str:
        return "master_data"

    @property
    def table(self) -> str:
        return "stores"

    def generate(self) -> pd.DataFrame:
        rows = []
        store_types = ["RETAIL", "OUTLET", "WAREHOUSE"]

        for store_number in range(1, Config.NUM_STORES + 1):
            city, state = random.choice(INDIAN_CITIES)

            created_timestamp = fake.date_time_between(
                start_date="-8y",
                end_date="-1y",
            )

            updated_timestamp = fake.date_time_between(
                start_date=created_timestamp,
                end_date="now",
            )

            rows.append(
                {
                    "store_name": f"RetailHub {city} {store_number:03d}",
                    "city": city,
                    "state": state,
                    "country": "India",
                    "store_type": random.choice(store_types),
                    "opened_date": created_timestamp.date(),
                    "created_timestamp": created_timestamp,
                    "updated_timestamp": updated_timestamp,
                    "source_system": "SQLSERVER",
                    "is_deleted": False,
                }
            )

        return pd.DataFrame(rows)