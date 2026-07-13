from sqlalchemy import text
from common.database import engine

with engine.connect() as conn:

    result = conn.execute(text("SELECT @@VERSION"))

    print(result.scalar())