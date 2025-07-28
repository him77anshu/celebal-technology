import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, count, window, desc, current_timestamp, to_timestamp, datediff, dayofmonth, month, year, expr
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType
import os

try:
    spark
except NameError:
    spark = SparkSession.builder \
        .appName("NYCTaxiAnalysis") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0") \
        .getOrCreate()

print("SparkSession initialized.")

csv_file_path = "/mnt/nyc_taxi_data/yellow_tripdata_2020-01.csv"

schema = StructType([
    StructField("VendorID", IntegerType(), True),
    StructField("tpep_pickup_datetime", TimestampType(), True),
    StructField("tpep_dropoff_datetime", TimestampType(), True),
    StructField("passenger_count", IntegerType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("RatecodeID", IntegerType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("PULocationID", IntegerType(), True),
    StructField("DOLocationID", IntegerType(), True),
    StructField("payment_type", IntegerType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("congestion_surcharge", DoubleType(), True)
])

try:
    df = spark.read \
        .option("header", "true") \
        .schema(schema) \
        .csv(csv_file_path)
    print(f"Data loaded from {csv_file_path}. Total rows: {df.count()}")
    df.printSchema()
    df.show(5)
except Exception as e:
    print(f"Error loading data: {e}")
    print("Please ensure the CSV file path is correct and accessible in your Spark environment.")
    exit()

parquet_output_path = "/mnt/nyc_taxi_data/processed_taxi_data.parquet"

print(f"\nWriting processed data to Parquet at: {parquet_output_path}")
df.write.mode("overwrite").parquet(parquet_output_path)
print("Data written to Parquet successfully.")

df_with_revenue = df.withColumn(
    "Revenue",
    col("fare_amount") + col("extra") + col("mta_tax") +
    col("improvement_surcharge") + col("tip_amount") + col("tolls_amount") +
    col("total_amount")
)
print("\n--- Query 1: DataFrame with 'Revenue' column ---")
df_with_revenue.select("fare_amount", "extra", "mta_tax", "improvement_surcharge", "tip_amount", "tolls_amount", "total_amount", "Revenue").show(5)

print("\n--- Query 2: Total passengers by Pickup Location ID (PULocationID) ---")
passenger_count_by_area = df_with_revenue.groupBy("PULocationID") \
                                        .agg(sum("passenger_count").alias("TotalPassengers")) \
                                        .orderBy(col("TotalPassengers").desc())
passenger_count_by_area.show(10)

print("\n--- Query 3: Realtime Average fare/total earning by Vendor (Simulated Streaming) ---")

streaming_df = spark.readStream \
    .option("header", "true") \
    .schema(schema) \
    .csv(csv_file_path)

avg_fare_by_vendor_stream = streaming_df \
    .groupBy("VendorID") \
    .agg(
        avg("fare_amount").alias("AverageFare"),
        sum("total_amount").alias("TotalEarnings")
    ) \
    .orderBy(col("TotalEarnings").desc())

query3_stream = avg_fare_by_vendor_stream.writeStream \
    .outputMode("complete") \
    .format("memory") \
    .queryName("avg_fare_by_vendor_query") \
    .trigger(processingTime='5 seconds') \
    .start()

print("Streaming query for Query 3 started. Waiting for data...")
import time
time.sleep(15)
print("\n--- Query 3: Current state of average fare/total earning by Vendor ---")
spark.sql("SELECT * FROM avg_fare_by_vendor_query").show(5)
query3_stream.stop()
print("Streaming query for Query 3 stopped.")

print("\n--- Query 4: Moving Count of payments by payment mode (Simulated Streaming) ---")

payment_mode_stream = streaming_df \
    .withWatermark("tpep_pickup_datetime", "10 minutes") \
    .groupBy(
        window(col("tpep_pickup_datetime"), "5 minutes", "1 minute"),
        col("payment_type")
    ) \
    .agg(count("*").alias("PaymentCount")) \
    .orderBy(col("window.start"), col("PaymentCount").desc())

query4_stream = payment_mode_stream.writeStream \
    .outputMode("append") \
    .format("memory") \
    .queryName("moving_payment_count_query") \
    .trigger(processingTime='5 seconds') \
    .start()

print("Streaming query for Query 4 started. Waiting for data...")
time.sleep(15)
print("\n--- Query 4: Current state of moving payment counts ---")
spark.sql("SELECT * FROM moving_payment_count_query").show(10)
query4_stream.stop()
print("Streaming query for Query 4 stopped.")

print("\n--- Query 5: Highest two gaining vendors on a particular date ---")
target_date = "2020-01-01"

highest_gaining_vendors = df_with_revenue.filter(col("tpep_pickup_datetime").cast("date") == target_date) \
                                        .groupBy("VendorID") \
                                        .agg(
                                            sum("passenger_count").alias("TotalPassengers"),
                                            sum("trip_distance").alias("TotalTripDistance"),
                                            sum("Revenue").alias("TotalRevenue")
                                        ) \
                                        .orderBy(col("TotalRevenue").desc()) \
                                        .limit(2)

print(f"Top 2 gaining vendors on {target_date}:")
highest_gaining_vendors.show()

print("\n--- Query 6: Most passengers between two locations (routes) ---")
most_passengers_route = df_with_revenue.groupBy("PULocationID", "DOLocationID") \
                                    .agg(sum("passenger_count").alias("TotalPassengers")) \
                                    .orderBy(col("TotalPassengers").desc()) \
                                    .limit(10)

print("Top 10 routes with most passengers:")
most_passengers_route.show()

print("\n--- Query 7: Get top pickup locations with most passengers in last 5/10 seconds (Simulated Streaming) ---")

top_pickup_locations_stream = streaming_df \
    .withWatermark("tpep_pickup_datetime", "20 seconds") \
    .groupBy(
        window(col("tpep_pickup_datetime"), "10 seconds", "5 seconds"),
        col("PULocationID")
    ) \
    .agg(sum("passenger_count").alias("TotalPassengers")) \
    .orderBy(col("window.start"), col("TotalPassengers").desc())

query7_stream = top_pickup_locations_stream.writeStream \
    .outputMode("append") \
    .format("memory") \
    .queryName("top_pickup_locations_query") \
    .trigger(processingTime='5 seconds') \
    .start()

print("Streaming query for Query 7 started. Waiting for data...")
time.sleep(15)
print("\n--- Query 7: Current state of top pickup locations (last 5/10 seconds) ---")
spark.sql("SELECT * FROM top_pickup_locations_query").show(10)
query7_stream.stop()
print("Streaming query for Query 7 stopped.")