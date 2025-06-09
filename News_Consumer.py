import json
import os
from kafka import KafkaConsumer
import time
time.sleep(15)


KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'news-articles')

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    group_id='my_group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True
)

for article in consumer:
    article = article.value
    print(f"Received article: {article['title']}")
    print(f"Published at: {article['publishedAt']}")
    print(f"Author: {article['author']}")
    print(f"Description: {article['description']}")
    print(f"URL: {article['url']}")
