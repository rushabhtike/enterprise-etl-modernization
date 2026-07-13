from dotenv import load_dotenv
import os

load_dotenv()

class Config:
    DB_SERVER = os.getenv("DB_SERVER")
    DB_PORT = int(os.getenv("DB_PORT","1433"))
    DB_DATABASE = os.getenv("DB_DATABASE")
    DB_USERNAME = os.getenv("DB_USERNAME")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    DB_DRIVER = os.getenv("DB_DRIVER")

    RANDOM_SEED = 42

    DEVELOPMENT = True

    if DEVELOPMENT:
        NUM_SUPPLIERS = 20
        NUM_STORES = 5
        NUM_PRODUCTS = 500
        NUM_CUSTOMERS = 1000
        NUM_EMPLOYEES = 50
        NUM_ORDERS = 10000
    else:
        NUM_SUPPLIERS = 100
        NUM_STORES = 25
        NUM_PRODUCTS = 5000
        NUM_CUSTOMERS = 100000
        NUM_EMPLOYEES = 500
        NUM_ORDERS = 1000000