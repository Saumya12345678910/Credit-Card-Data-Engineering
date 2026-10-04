from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when, to_timestamp
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    LongType
)


# Create Spark session
spark = (
    SparkSession.builder
    .appName("CreditCardKafkaStreaming")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# Schema of incoming Kafka transaction JSON
transaction_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("card_id", IntegerType(), True),
    StructField("amount", DoubleType(), True),
    StructField("merchant_id", LongType(), True),
    StructField("merchant_city", StringType(), True),
    StructField("merchant_state", StringType(), True),
    StructField("mcc", IntegerType(), True),
    StructField("payment_method", StringType(), True),
    StructField("fraud_status", StringType(), True),
    StructField("transaction_status", StringType(), True),
    StructField("transaction_timestamp", StringType(), True)
])


# Read continuously from Kafka
kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "credit-card-kafka:29092")
    .option("subscribe", "credit_card_transactions")
    .option("startingOffsets", "latest")
    .load()
)


# Convert Kafka JSON value into structured columns
transactions_df = (
    kafka_df
    .select(
        from_json(
            col("value").cast("string"),
            transaction_schema
        ).alias("data")
    )
    .select("data.*")
)
processed_df = (
    transactions_df
    .withColumn(
        "transaction_timestamp",
        to_timestamp(col("transaction_timestamp"))
    )
    .withColumn(
        "fraud_flag",
        when(col("fraud_status") == "Yes", 1).otherwise(0)
    )
    .withColumn(
        "is_high_value",
        when(col("amount") >= 300, 1).otherwise(0)
    )
)

# Display incoming transactions
query = (
    processed_df.writeStream
    .format("parquet")
    .outputMode("append")
    .option("path", "/opt/project/streaming_output")
    .option("checkpointLocation", "/opt/project/checkpoints/transactions")
    .start()
)

query.awaitTermination()
