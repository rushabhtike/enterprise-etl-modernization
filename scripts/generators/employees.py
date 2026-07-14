import random

import pandas as pd
from faker import Faker
from sqlalchemy import text

from common.config import Config
from common.database import engine
from common.reference_data import JOB_TITLES
from generators.base_generator import BaseGenerator


fake = Faker("en_IN")
random.seed(Config.RANDOM_SEED)
Faker.seed(Config.RANDOM_SEED)


class EmployeesGenerator(BaseGenerator):

    @property
    def schema(self) -> str:
        return "hr"

    @property
    def table(self) -> str:
        return "employees"

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

        if not store_ids:
            raise ValueError("Stores must be loaded before employees.")

        rows = []

        for employee_number in range(1, Config.NUM_EMPLOYEES + 1):
            first_name = fake.first_name()
            last_name = fake.last_name()

            created_timestamp = fake.date_time_between(
                start_date="-6y",
                end_date="-30d",
            )

            updated_timestamp = fake.date_time_between(
                start_date=created_timestamp,
                end_date="now",
            )

            rows.append(
                {
                    "store_id": random.choice(store_ids),
                    "first_name": first_name,
                    "last_name": last_name,
                    "email": (
                        f"{first_name}.{last_name}.{employee_number}"
                        "@retailhub.example"
                    ).lower().replace(" ", ""),
                    "phone": fake.phone_number(),
                    "job_title": random.choice(JOB_TITLES),
                    "hire_date": created_timestamp.date(),
                    "salary": round(random.uniform(240000, 1500000), 2),
                    "manager_id": None,
                    "created_timestamp": created_timestamp,
                    "updated_timestamp": updated_timestamp,
                    "source_system": "SQLSERVER",
                    "is_deleted": random.random() < 0.01,
                }
            )

        return pd.DataFrame(rows)