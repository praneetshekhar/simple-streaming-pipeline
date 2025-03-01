from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("KafkaToPostgres") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,org.apache.kafka:kafka-clients:3.2.0") \
    .getOrCreate()


# Define schema
schema = StructType() \
    .add("symbol", StringType()) \
    .add("price", DoubleType()) \
    .add("timestamp", DoubleType())


# Read Kafka Stream
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "stock_topic") \
    .option("startingOffsets", "earliest") \
    .load()

# Parse JSON
df_parsed = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")


# Write to PostgreSQL
df_parsed.writeStream \
    .foreachBatch(lambda batch_df, batch_id: batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/stock_data") \
        .option("dbtable", "stock_prices_live") \
        .option("user", "admin") \
        .option("password", "password") \
        .mode("append") \
        .save()) \
    .start() \
    .awaitTermination()

