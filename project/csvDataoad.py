df = spark.read.format("csv").option("header", True).schema(schema).load("/mnt/customers/raw/customers.csv")
df.createOrReplaceTempView("vw_customers_csv")
spark.sql("""
  CREATE OR REPLACE TABLE catalog.schema.delta_customers_csv
  USING DELTA
  AS SELECT * FROM vw_customers_csv
""")
