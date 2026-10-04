import json
import time
import random
from datetime import datetime

from confluent_kafka import Producer


# Kafka broker configuration
producer_config = {
    "bootstrap.servers": "localhost:9092"
}

producer = Producer(producer_config)

topic_name = "credit_card_transactions"


def delivery_report(err, msg):
    """Called after Kafka attempts to deliver a message."""

    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(
            f"Delivered to {msg.topic()} "
            f"[partition {msg.partition()}]"
        )


for i in range(1, 11):

    transaction = {
        "transaction_id": f"STREAM_TXN_{i + 1:03d}",
        "user_id": random.randint(0, 1999),
        "card_id": random.randint(0, 4),
        "amount": round(random.uniform(5, 500), 2),
        "merchant_id": random.randint(10000, 99999),
        "merchant_city": random.choice(
            ["Bangalore", "Mumbai", "Delhi", "Chennai"]
        ),
        "merchant_state": random.choice(
            ["KA", "MH", "DL", "TN"]
        ),
        "mcc": random.choice([5411, 5812, 5912, 5999]),
        "payment_method": random.choice(
            ["Chip", "Swipe", "Online"]
        ),
        "fraud_status": random.choice(
            ["No", "No", "No", "Yes"]
        ),
        "transaction_status": "Success",
        "transaction_timestamp": datetime.now().isoformat()
    }

    producer.produce(
        topic=topic_name,
        key=str(transaction["user_id"]),
        value=json.dumps(transaction),
        callback=delivery_report
    )

    producer.poll(0)

    print("Sent:", transaction)

    time.sleep(2)


producer.flush()

print("\nAll transactions sent successfully.")
