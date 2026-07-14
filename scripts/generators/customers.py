import random

import pandas as pd
from faker import Faker

from common.config import Config
from common.reference_data import INDIAN_CITIES
from generators.base_generator import BaseGenerator


fake = Faker("en_IN")
random.seed(Config.RANDOM_SEED)
Faker.seed(Config.RANDOM_SEED)


class CustomersGenerator(BaseGenerator):

    @property
    def schema(self) -> str:
        return "master_data"

    @property
    def table(self) -> str:
        return "customers"

    def generate(self) -> pd.DataFrame:
        rows = []

        for customer_number in range(1, Config.NUM_CUSTOMERS + 1):
            city, state = random.choice(INDIAN_CITIES)

            first_name = fake.first_name()
            last_name = fake.last_name()

            created_timestamp = fake.date_time_between(
                start_date="-5y",
                end_date="-30d",
            )

            updated_timestamp = fake.date_time_between(
                start_date=created_timestamp,
                end_date="now",
            )

            phone = fake.phone_number()

            if random.random() < 0.05:
                phone = None

            email_name = (
                f"{first_name}.{last_name}.{customer_number}"
                .lower()
                .replace(" ", "")
                .replace("'", "")
            )

            rows.append(
                {
                    "first_name": first_name,
                    "last_name": last_name,
                    "email": f"{email_name}@example.com",
                    "phone": phone,
                    "date_of_birth": fake.date_of_birth(
                        minimum_age=18,
                        maximum_age=80,
                    ),
                    "gender": random.choice(["M", "F", "O"]),
                    "city": city,
                    "state": state,
                    "country": "India",
                    "created_timestamp": created_timestamp,
                    "updated_timestamp": updated_timestamp,
                    "source_system": "SQLSERVER",
                    "is_deleted": random.random() < 0.01,
                }
            )

        return pd.DataFrame(rows)