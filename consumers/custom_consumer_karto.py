import json
import sqlite3
import os
import matplotlib.pyplot as plt
from kafka import KafkaConsumer
from dotenv import load_dotenv
from collections import defaultdict

# Load .env variables
load_dotenv()
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "buzzline05_topic")
KAFKA_SERVER = os.getenv("KAFKA_SERVER", "localhost:9092")
DB_FILE = "data/messages.sqlite"

# Initialize DB
def init_db():
    os.makedirs("data", exist_ok=True)
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            author TEXT,
            message TEXT,
            category TEXT,
            timestamp TEXT
        )
    ''')
    conn.commit()
    conn.close()

# Insert message into SQLite
def store_message(msg):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO messages (author, message, category, timestamp)
        VALUES (?, ?, ?, ?)
    ''', (msg["author"], msg["message"], msg["category"], msg["timestamp"]))
    conn.commit()
    conn.close()

# Initialize Kafka consumer
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_SERVER,
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True
)

# Visualization setup
category_counts = defaultdict(int)
plt.ion()
fig, ax = plt.subplots()

def update_plot():
    ax.clear()
    ax.bar(category_counts.keys(), category_counts.values(), color='skyblue')
    ax.set_xlabel("Category")
    ax.set_ylabel("Count")
    ax.set_title("Live Category Counts")
    plt.pause(0.1)

def main():
    init_db()
    print(f"Consuming messages from topic: {KAFKA_TOPIC}")
    try:
        for msg in consumer:
            message = msg.value
            store_message(message)
            category_counts[message["category"]] += 1
            update_plot()
            print(f"Consumed: {message}")
    except KeyboardInterrupt:
        print("Consumer stopped.")
    finally:
        consumer.close()
        plt.ioff()
        plt.show()

if __name__ == "__main__":
    main()
