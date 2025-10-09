import json
import sqlite3
import os
import matplotlib.pyplot as plt
from kafka import KafkaConsumer
from dotenv import load_dotenv
from collections import defaultdict
import time

# Load environment variables
load_dotenv()
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "buzzline05_topic")
KAFKA_SERVER = os.getenv("KAFKA_SERVER", "localhost:9092")
DB_FILE = "data/messages.sqlite"

# Initialize database
def init_db():
    os.makedirs("data", exist_ok=True)
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            author TEXT,
            message TEXT,
            category TEXT,
            timestamp TEXT
        )
    """)
    conn.commit()
    conn.close()

def store_message(msg):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO messages (author, message, category, timestamp)
        VALUES (?, ?, ?, ?)
    """, (msg["author"], msg["message"], msg["category"], msg["timestamp"]))
    conn.commit()
    conn.close()

# Kafka Consumer setup
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_SERVER,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=True
)

# Tracking data
category_counts = defaultdict(int)
author_counts = defaultdict(int)
start_time = time.time()
total_messages = 0

# Visualization setup
plt.ion()
fig, (ax1, ax2, ax3) = plt.subplots(1, 3, figsize=(16, 5))
plt.tight_layout(pad=4)

def update_plots():
    # --- Bar chart: category counts ---
    ax1.clear()
    ax1.bar(category_counts.keys(), category_counts.values(), color="royalblue", edgecolor="black")
    ax1.set_title("Live Category Counts", fontsize=11)
    ax1.set_xlabel("Category")
    ax1.set_ylabel("Messages")
    ax1.tick_params(axis='x', rotation=45)

    # --- Pie chart: category distribution ---
    ax2.clear()
    if category_counts:
        ax2.pie(category_counts.values(), labels=category_counts.keys(), autopct='%1.1f%%', startangle=90)
        ax2.set_title("Category Distribution", fontsize=11)

    # --- Horizontal bar chart: top authors ---
    ax3.clear()
    if author_counts:
        top_authors = sorted(author_counts.items(), key=lambda x: x[1], reverse=True)[:5]
        names, counts = zip(*top_authors)
        ax3.barh(names, counts, color="mediumseagreen", edgecolor="black")
        ax3.set_title("Top 5 Active Authors", fontsize=11)
        ax3.set_xlabel("Message Count")
        ax3.invert_yaxis()  # Highest author on top

    plt.pause(0.1)

def main():
    init_db()
    print(f"Consuming messages from topic: {KAFKA_TOPIC}")
    global total_messages
    try:
        for msg in consumer:
            message = msg.value
            store_message(message)
            category_counts[message["category"]] += 1
            author_counts[message["author"]] += 1
            total_messages += 1
            update_plots()
            print(f"[{total_messages}] Consumed: {message}")
    except KeyboardInterrupt:
        print("Consumer stopped.")
    finally:
        consumer.close()
        plt.ioff()
        plt.show()

if __name__ == "__main__":
    main()
