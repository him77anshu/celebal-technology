def validate_delta_table_count(table_names, expected_count=500):
    for table in table_names:
        row_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {table}").collect()[0]['cnt']
        assert row_count == expected_count, f"Table {table} does not have {expected_count} rows (found {row_count})"
    print("All tables validated for row count.")

# Usage:
tables = [
    "catalog.schema.delta_customers_managed",
    "catalog.schema.delta_customers_parquet",
    # ... add all created table names
]
validate_delta_table_count(tables)
