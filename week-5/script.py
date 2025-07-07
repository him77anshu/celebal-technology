import pandas as pd
from sqlalchemy import create_engine
from fastavro import writer, parse_schema
import os

engine = create_engine("sqlite:///himanshu.db")
query = "SELECT * FROM my_table"
df = pd.read_sql(query, engine)
os.makedirs("output", exist_ok=True)
df.to_csv("output/data.csv", index=False)
df.to_parquet("output/data.parquet", index=False)

def infer_avro_schema(df):
    fields = []
    for column in df.columns:
        if pd.api.types.is_integer_dtype(df[column]):
            typ = 'long'
        elif pd.api.types.is_float_dtype(df[column]):
            typ = 'double'
        elif pd.api.types.is_bool_dtype(df[column]):
            typ = 'boolean'
        else:
            typ = 'string'
        fields.append({'name': column, 'type': [typ, 'null']})
    return {
        'type': 'record',
        'name': 'MyTableRecord',
        'fields': fields
    }

schema = parse_schema(infer_avro_schema(df))
records = df.to_dict(orient='records')

with open("output/data.avro", "wb") as out_file:
    writer(out_file, schema, records)