from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Create Spark Session
spark = SparkSession.builder \
    .appName("Target Data Processing") \
    .master("local[*]") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.sql.warehouse.dir", "C:/spark-warehouse") \
    .enableHiveSupport() \
    .getOrCreate()

# Read JSON Data
df = spark.read.json(
    r"C:\Users\nikhi\OneDrive\Desktop\target-data-engineering\output\kafka_orders.json"
)

# Print Schema
print("Schema:")
df.printSchema()

# Show Sample Data
print("Sample Data:")
df.show(5)

df = df.dropna(subset=[
    "order_products_value",
    "order_freight_value",
    "order_status",
    "customer_city",
    "customer_state"
])

# Mean Calculations
print("Average Values:")
df.select(
    mean("order_products_value").alias("avg_product_value"),
    mean("order_freight_value").alias("avg_freight_value")
).show()

# Order Status Distribution
print("Order Status Distribution:")
df.groupBy("order_status").count().show()

# Unique States
print("Unique States:")
df.select("customer_state").distinct().show()

# Top 5 Cities with Most Orders
print("Top 5 Cities:")
df.groupBy("customer_city") \
    .count() \
    .orderBy(col("count").desc()) \
    .show(5)

# Missing Values Count
print("Missing Values Count:")

for column in df.columns:
    missing_count = df.filter(col(column).isNull()).count()
    print(f"{column}: {missing_count}")


# Order Status Percentage
print("Order Status Percentage:")

total_orders = df.count()

df.groupBy("order_status") \
    .count() \
    .withColumn(
        "percentage",
        (col("count") / total_orders) * 100
    ) \
    .show()


# Total Sales by City
print("Total Sales by City:")

df.groupBy("customer_city") \
    .agg(
        sum("order_products_value")
        .alias("total_sales")
    ) \
    .orderBy(col("total_sales").desc()) \
    .show(10)


# Correlation Analysis
print("Correlation Analysis:")

corr_value = df.stat.corr(
    "order_products_value",
    "order_freight_value"
)

print(
    f"Correlation between Product Value and Freight Value: {corr_value}"
)
# # Remove rows where date columns have NaN values
df = df.filter(
    (col("order_purchase_timestamp") != "NaN") &
    (col("order_aproved_at") != "NaN") &
    (col("order_delivered_customer_date") != "NaN")
)

# Convert date columns to timestamp
df = df.withColumn("order_purchase_timestamp", to_timestamp("order_purchase_timestamp")) \
       .withColumn("order_aproved_at", to_timestamp("order_aproved_at")) \
       .withColumn("order_delivered_customer_date", to_timestamp("order_delivered_customer_date"))

# Delivery and approval time analysis
df = df.withColumn(
    "delivery_time_days",
    datediff(col("order_delivered_customer_date"), col("order_purchase_timestamp"))
).withColumn(
    "approval_time_hours",
    (unix_timestamp("order_aproved_at") - unix_timestamp("order_purchase_timestamp")) / 3600
)

print("Average Delivery Time and Approval Time:")
df.select(
    avg("delivery_time_days").alias("avg_delivery_days"),
    avg("approval_time_hours").alias("avg_approval_hours")
).show()

print("Average Review Score:")
df.select(
    avg("review_score").alias("avg_review_score")
).show()

print("Top 3 Fastest Delivery Cities:")
df.filter(col("delivery_time_days").isNotNull()) \
  .groupBy("customer_city") \
  .agg(avg("delivery_time_days").alias("avg_delivery_days")) \
  .orderBy(col("avg_delivery_days").asc()) \
  .show(3)

print("Top 3 Slowest Delivery Cities:")
df.filter(col("delivery_time_days").isNotNull()) \
  .groupBy("customer_city") \
  .agg(avg("delivery_time_days").alias("avg_delivery_days")) \
  .orderBy(col("avg_delivery_days").desc()) \
  .show(3)

print("Relation Between Delivery Time and Review Score:")
df.filter(col("delivery_time_days").isNotNull()) \
  .groupBy("review_score") \
  .agg(avg("delivery_time_days").alias("avg_delivery_days")) \
  .orderBy("review_score") \
  .show()

print("Correlation Between Delivery Time and Review Score:")
delivery_review_corr = df.stat.corr("delivery_time_days", "review_score")
print(f"Correlation between Delivery Time and Review Score: {delivery_review_corr}")
# Save DataFrame as Hive table
df.write.mode("overwrite").saveAsTable("retail_orders")

print("Hive table 'retail_orders' created successfully.")

# Verify Hive table
spark.sql("SHOW TABLES").show()

# Show sample records from Hive table
spark.sql("SELECT * FROM retail_orders LIMIT 5").show()
spark.stop()

