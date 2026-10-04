# Credit-Card-Data-Engineering
End-to-end data engineering pipeline using PySpark, Databricks, Delta Lake, Airflow, Kafka and Power BI.

## Real-Time Streaming Pipeline — Kafka & Spark Structured Streaming

In addition to the batch data pipeline, a real-time streaming pipeline was implemented to simulate and process incoming credit-card transactions.

### Streaming Architecture

Python Producer → Apache Kafka → Spark Structured Streaming → Parquet Output

### Implementation

- Deployed Apache Kafka locally using Docker in KRaft mode.
- Created the `credit_card_transactions` Kafka topic with 3 partitions.
- Developed a Python producer to simulate incoming credit-card transaction events.
- Published transaction events as JSON using `user_id` as the Kafka message key.
- Used Spark Structured Streaming to continuously consume events from Kafka.
- Parsed JSON messages using `from_json()` with a predefined Spark schema.
- Applied streaming transformations including timestamp conversion, fraud flag creation, and high-value transaction identification.
- Persisted processed streaming data in Parquet format.
- Implemented checkpointing for streaming query recovery and progress tracking.
- Validated record counts, schema, and transformation results.

### Streaming Components

- `kafka/docker-compose.yml` — Apache Kafka Docker configuration
- `kafka/producer/transaction_producer.py` — Python Kafka producer
- `kafka/spark_streaming/transaction_stream.py` — Spark Structured Streaming pipeline
