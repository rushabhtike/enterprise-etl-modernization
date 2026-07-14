import random

import pandas as pd
from faker import Faker

from common.config import Config
from common.reference_data import SUPPLIER_PREFIXES, SUPPLIER_TYPES
from generators.base_generator import BaseGenerator


fake = Faker("en_IN")
random.seed(Config.RANDOM_SEED)
Faker.seed(Config.RANDOM_SEED)


class SuppliersGenerator(BaseGenerator):

    @property
    def schema(self) -> str:
        return "master_data"

    @property
    def table(self) -> str:
        return "suppliers"

    def generate(self) -> pd.DataFrame:
        rows = []
        used_supplier_names: set[str] = set()

        while len(rows) < Config.NUM_SUPPLIERS:
            prefix = random.choice(SUPPLIER_PREFIXES)
            supplier_type = random.choice(SUPPLIER_TYPES)

            base_name = f"{prefix} {supplier_type}"

            if base_name in used_supplier_names:
                continue

            used_supplier_names.add(base_name)

            supplier_name = f"{base_name} Pvt Ltd"
            email_domain = (
                base_name.lower()
                .replace(" ", "")
                .replace("&", "and")
            )

            created_timestamp = fake.date_time_between(
                start_date="-2y",
                end_date="-6m",
            )

            updated_timestamp = fake.date_time_between(
                start_date=created_timestamp,
                end_date="now",
            )

            phone = fake.phone_number()

            if random.random() < 0.02:
                phone = None

            rows.append(
                {
                    "supplier_name": supplier_name,
                    "contact_name": fake.name(),
                    "email": f"contact@{email_domain}.com",
                    "phone": phone,
                    "country": "India",
                    "created_timestamp": created_timestamp,
                    "updated_timestamp": updated_timestamp,
                    "source_system": "SQLSERVER",
                    "is_deleted": False,
                }
            )

        return pd.DataFrame(rows)