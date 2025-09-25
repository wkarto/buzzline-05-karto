import json
import sqlite3
import time
from pathlib import Path

# Paths
BASE_DIR = Path(__file__).parent.parent
LIVE_DATA_PATH = BASE_DIR / "data" / "project_live.json"
DB_PATH = BASE_DIR / "data" / "buzz_p5.sqlite"

# Ensure database exists and table is created
conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()
cur.execute("""
CREATE TABLE IF NOT EXISTS message_insights (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    message TEXT,
    author TEXT,
    timestamp TEXT,
    category TEXT,
    sentiment REAL
)
""")
conn.commit()

def process_message(message_json):
    """
    Process a single message JSON and store insights in SQLite.
    """
    message = message_json.get("message")
    author = message_json.get("author")
    timestamp = message_json.get("timestamp")
    category = message_json.get("category")
    sentiment = message_json.get("sentiment")

    # Insert into SQLite
    cur.execute("""
    INSERT INTO message_insights (message, author, timestamp, category, sentiment)
    VALUES (?, ?, ?, ?, ?)
    """, (message, author, timestamp, category, sentiment))
    conn.commit()
    print(f"Processed message from {author} [{category}] with sentiment {sentiment}")

def tail_live_file(file_path):
    """
    Continuously read new messages appended to the live JSON file.
    """
    file_path = Path(file_path)
    if not file_path.exists():
        print(f"Live data file not found: {file_path}")
        return

    # Keep track of lines processed
    last_position = 0
    while True:
        with open(file_path, "r", encoding="utf-8") as f:
            f.seek(last_position)
            lines = f.readlines()
            last_position = f.tell()

        for line in lines:
            line = line.strip()
            if line:
                try:
                    message_json = json.loads(line)
                    process_message(message_json)
                except json.JSONDecodeError as e:
                    print(f"Invalid JSON: {line} | Error: {e}")
        time.sleep(1)  # Poll every second

if __name__ == "__main__":
    print("Starting Karto’s custom consumer...")
    tail_live_file(LIVE_DATA_PATH)
