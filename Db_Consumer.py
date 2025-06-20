
import os
import json
import time
from kafka import KafkaConsumer
from Db import Article, SessionLocal

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "news-transformed")

while True:
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BROKER,
            group_id="my_group2",
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            auto_offset_reset="earliest",
            enable_auto_commit=True
        )
        print("[INFO] Connected to Kafka.")
        break
    except Exception as e:
        print(f"[WARN] Kafka not ready: {e}. Retrying in 5 seconds...")
        time.sleep(5)

session = SessionLocal()

print("[INFO] Listening for transformed messages...")
for msg in consumer:
    data = msg.value
    print(f"[INFO] Saving to DB: {data['title']}")

    try:
        article = Article(
            title=data["title"],
            description=data["description"]
        )
        session.add(article)
        session.commit()
        print("[INFO] Saved.\n")
    except Exception as e:
        session.rollback()
        print(f"[ERROR] DB error: {e}")
