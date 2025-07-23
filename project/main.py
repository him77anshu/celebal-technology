def load_and_validate(
    file_path, 
    file_format, 
    table_name, 
    schema=None, 
    temp_view_name=None, 
    expected_count=500
):
    supported_direct_ctas = ["parquet", "avro", "orc", "delta"]
    if file_format in supported_direct_ctas:
        # Use SQL CTAS directly
        sql = f"""
        CREATE OR REPLACE TABLE {table_name}
        USING DELTA
        AS SELECT * FROM {file_format}.`{file_path}`
        """
        spark.sql(sql)
    else:
        # Read to DataFrame with provided schema
        df = spark.read.format(file_format).option("header", True)
        if schema:
            df = df.schema(schema)
        df = df.load(file_path)
        temp_view = temp_view_name or "temp_view"
        df.createOrReplaceTempView(temp_view)
        spark.sql(f"""
            CREATE OR REPLACE TABLE {table_name}
            USING DELTA
            AS SELECT * FROM {temp_view}
        """)
    # Validation
    cnt = spark.sql(f"SELECT COUNT(*) as cnt FROM {table_name}").collect()[0]['cnt']
    assert cnt == expected_count, f"{table_name} has {cnt} rows, expected {expected_count}"
    print(f"{table_name} loaded and validated ({cnt} rows)")

# Example usage:
load_and_validate(
    file_path="/mnt/customers/raw/customers.csv",
    file_format="csv",
    table_name="catalog.schema.delta_customers_csv",
    schema=schema
)
