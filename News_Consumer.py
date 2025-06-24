import json
import os
import time
import psycopg2
from kafka import KafkaConsumer
import uuid

# Configuration
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'news-transformed')
CONSUMER_MODE = os.getenv('CONSUMER_MODE', 'print')

# PostgreSQL settings
DB_NAME = os.getenv('POSTGRES_DB', 'newsdb')
DB_USER = os.getenv('POSTGRES_USER', 'newsuser')
DB_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'news123')
DB_HOST = os.getenv('POSTGRES_HOST', 'postgres')
DB_PORT = os.getenv('POSTGRES_PORT', 5432)

conn = None
cursor = None

def wait_for_postgres(max_retries=10, delay=5):
    for attempt in range(max_retries):
        try:
            conn = psycopg2.connect(
                dbname=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD,
                host=DB_HOST,
                port=DB_PORT
            )
            print("[DB] Connected to PostgreSQL.")
            return conn
        except Exception as e:
            print(f"[DB WAIT] Attempt {attempt + 1}/{max_retries}: {e}")
            time.sleep(delay)
    print("[DB ERROR] Could not connect to PostgreSQL after retries.")
    exit(1)

def setup_database():
    global conn, cursor
    conn = wait_for_postgres()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS news_articles (
                id SERIAL PRIMARY KEY,
                title TEXT,
                description TEXT
            )
        """)
        conn.commit()
        print("[DB] Table 'news_articles' is ready.")
    except Exception as e:
        print(f"[DB ERROR] Failed to create table: {e}")
        conn.rollback()
        exit(1)

def insert_article_to_db(title, description):
    try:
        cursor.execute(
            "INSERT INTO news_articles (title, description) VALUES (%s, %s)",
            (title, description)
        )
        conn.commit()
        print("[DB] Inserted article.")
    except Exception as e:
        print(f"[DB ERROR] Failed to insert article: {e}")
        conn.rollback()

def process_message(article):
    title = article.get("title", "")
    description = article.get("description", "")

    if CONSUMER_MODE == "db":
        insert_article_to_db(title, description)
    elif CONSUMER_MODE == "print":
        print(f"Title: {title}")
        print(f"Description: {description}")
        print("-----")
    else:
        print(f"[WARN] Unknown CONSUMER_MODE: {CONSUMER_MODE}")

def start_kafka_consumer():
    print("[Kafka] Waiting to connect...")
    while True:
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BROKER,
                group_id=f'my_group2-{uuid.uuid4()}',
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                auto_offset_reset='earliest',
                enable_auto_commit=True
            )
            print(f"[Kafka] Connected and subscribed to topic '{KAFKA_TOPIC}'.")
            break
        except Exception as e:
            print("Kafka not available, retrying in 5 seconds...")
            time.sleep(5)

    for message in consumer:
        process_message(message.value)

if __name__ == '__main__':
    time.sleep(10)  # Optional wait for services to be ready
    if CONSUMER_MODE == "db":
        setup_database()
    start_kafka_consumer()
