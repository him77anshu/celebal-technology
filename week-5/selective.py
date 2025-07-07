import pandas as pd
from sqlalchemy import create_engine
SOURCE_DB = "mysql+pymysql://user:admin123@localhost:3306/himanshu_db"
DEST_DB   = "mysql+pymysql://user:admin123@localhost:3306/temp_db"

tables_to_copy = {
    'employees': ['id', 'name', 'email'],        
    'departments': ['dept_id', 'dept_name']      
}

src_engine = create_engine(SOURCE_DB)
dst_engine = create_engine(DEST_DB)

for table, columns in tables_to_copy.items():
    print(f"Copying from table: {table} → columns: {columns}")
    
    
    col_str = ", ".join(columns)
    query = f"SELECT {col_str} FROM {table}"
    df = pd.read_sql(query, src_engine)
    df.to_sql(table, dst_engine, if_exists='replace', index=False)
    
    print(f"Copied {len(df)} rows to '{table}'")

print("\n Selective table + column migration complete!")
