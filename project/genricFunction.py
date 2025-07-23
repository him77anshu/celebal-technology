def load_file_to_delta(file_path, file_format, table_name, schema=None, temp_view=None, expected_rows=500):
    direct_ctas_formats = ['parquet', 'avro', 'orc', 'delta']
    if file_format.lower() in direct_ctas_formats:
        spark.sql(f"""
            CREATE OR REPLACE TABLE {table_name}
            USING DELTA
            AS SELECT * FROM {file_format}.`{file_path}`
        """)
    else:
        df = spark.read.format(file_format).option("header", True)
        if schema:
            df = df.schema(schema)
        df = df.load(file_path)
        view_name = temp_view if temp_view else f"temp_view_{table_name}"
        df.createOrReplaceTempView(view_name)
        spark.sql(f"""
            CREATE OR REPLACE TABLE {table_name}
            USING DELTA
            AS SELECT * FROM {view_name}
        """)
    
    # Validation
    count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {table_name}").collect()[0]['cnt']
    if count == expected_rows:
        print(f"✅ {table_name} loaded and validated with {count} rows.")
    else:
        raise Exception(f"❌ {table_name} validation error: {count} rows found, expected {expected_rows}.")

# Example usage for CSV:
load_file_to_delta("/mnt/customers/raw/customers.csv", "csv", "delta_customers_csv", schema=schema)
