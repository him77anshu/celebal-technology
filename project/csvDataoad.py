from pyspark.sql.types import StructType, StructField, IntegerType, StringType

schema = StructType([
    StructField("CustomerID", IntegerType(), True),
    StructField("FirstName", StringType(), True),
    StructField("LastName", StringType(), True),
    StructField("Email", StringType(), True),
    StructField("Phone", StringType(), True),
    StructField("City", StringType(), True),
    StructField("Country", StringType(), True),
    StructField("Age", IntegerType(), True)
])

df_csv = spark.read.format("csv") \
    .option("header", True) \
    .schema(schema) \
    .load("/mnt/customers/raw/customers.csv")

# Create Temp View
df_csv.createOrReplaceTempView("vw_customers_csv")

# Create managed Delta Table using CTAS from TempView
spark.sql("""
    CREATE OR REPLACE TABLE delta_customers_csv
    USING DELTA
    AS SELECT * FROM vw_customers_csv
""")
