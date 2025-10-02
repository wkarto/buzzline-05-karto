import sqlite3
import os

DB_FILE = "data/messages.sqlite"

def init_db(db_file=DB_FILE):
    os.makedirs("data", exist_ok=True)
    conn = sqlite3.connect(db_file)
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

def insert_message(msg, db_file=DB_FILE):
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO messages (author, message, category, timestamp)
        VALUES (?, ?, ?, ?)
    ''', (msg["author"], msg["message"], msg["category"], msg["timestamp"]))
    conn.commit()
    conn.close()
