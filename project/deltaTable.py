from pyspark.sql.types import StructType, StructField, IntegerType, StringType

schema = StructType([
    StructField("CustomerID", IntegerType(), True),
    StructField("FirstName", StringType(), True),
    StructField("LastName", StringType(), True),
    # ...add all fields
])

df = spark.read.format("csv").option("header", True).schema(schema).load("/mnt/customers/raw/customers.csv")
df.write.format("delta").mode("overwrite").saveAsTable("catalog.schema.delta_customers_managed")
