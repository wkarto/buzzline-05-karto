import json
import time
import os
import random
from kafka import KafkaProducer
from dotenv import load_dotenv
from datetime import datetime

# Load .env variables
load_dotenv()
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "buzzline05_topic")
KAFKA_SERVER = os.getenv("KAFKA_SERVER", "localhost:9092")

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

categories = ["payment", "cashout", "transfer", "debit", "credit"]
authors = [f"user_{i}" for i in range(1, 6)]

def generate_message():
    return {
        "author": random.choice(authors),
        "message": f"{random.choice(categories)} transaction",
        "category": random.choice(categories),
        "timestamp": datetime.now().isoformat()
    }

def main():
    print(f"Producing messages to topic: {KAFKA_TOPIC}")
    try:
        while True:
            msg = generate_message()
            producer.send(KAFKA_TOPIC, value=msg)
            print(f"Produced: {msg}")
            time.sleep(2)
    except KeyboardInterrupt:
        print("Producer stopped.")
    finally:
        producer.flush()
        producer.close()

if __name__ == "__main__":
    main()
