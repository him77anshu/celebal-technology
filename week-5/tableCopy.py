import pandas as pd
from sqlalchemy import create_engine, inspect

SOURCE_DB_URL = "sqlite:///himanshu.db"
DEST_DB_URL   = "sqlite:///temp.db"

src_engine = create_engine(SOURCE_DB_URL)
dst_engine = create_engine(DEST_DB_URL)
inspector = inspect(src_engine)
table_names = inspector.get_table_names()

print(f"Found tables: {table_names}")
for table in table_names:
    print(f"Copying table: {table}")
    df = pd.read_sql_table(table, src_engine)
    df.to_sql(table, dst_engine, if_exists='replace', index=False)
    print(f"{table} copied.")

print("All tables copied successfully.")