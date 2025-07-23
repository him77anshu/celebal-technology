
--Loading AVRO, PARQUET, ORC & DELTA Using CTAS

-- Parquet
CREATE OR REPLACE TABLE delta_customers_parquet
USING DELTA
AS SELECT * FROM parquet.`/mnt/customers/raw/customers.parquet`;

-- Avro
CREATE OR REPLACE TABLE delta_customers_avro
USING DELTA
AS SELECT * FROM avro.`/mnt/customers/raw/customers.avro`;

-- ORC
CREATE OR REPLACE TABLE delta_customers_orc
USING DELTA
AS SELECT * FROM orc.`/mnt/customers/raw/customers.orc`;

-- Delta
CREATE OR REPLACE TABLE delta_customers_delta
USING DELTA
AS SELECT * FROM delta.`/mnt/customers/raw/customers.delta`;

