
CREATE OR REPLACE TABLE catalog.schema.delta_customers_parquet
USING DELTA
AS SELECT * FROM parquet.`/mnt/customers/raw/customers.parquet`;
CREATE OR REPLACE TABLE catalog.schema.delta_customers_avro
USING DELTA
AS SELECT * FROM avro.`/mnt/customers/raw/customers.avro`;

