
def validate_row_counts(tables, expected_count=500):
    """
    Validates that each Delta table in the 'tables' list has exactly 'expected_count' rows.
    Raises Exception if validation fails for any table.
    """
    for table_name in tables:
        count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {table_name}").collect()[0]['cnt']
        if count == expected_count:
            print(f"✅ Table {table_name} has {count} rows — validation passed.")
        else:
            print(f"❌ Table {table_name} has {count} rows, expected {expected_count} — validation failed.")
            raise Exception(f"Validation failed for {table_name}")

# List of Delta tables to validate
tables_to_validate = [
    "delta_customers_csv",
    "delta_customers_parquet",
    "delta_customers_avro",
    "delta_customers_orc",
    "delta_customers_delta"
]


validate_row_counts(tables_to_validate)
