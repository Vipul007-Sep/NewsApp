import json 
import os
from kafka import KafkaConsumer, KafkaProducer
import time
import uuid

def start_transformer():
    KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
    INPUT_TOPIC = os.getenv('KAFKA_INPUT_TOPIC', 'news-articles')
    OUTPUT_TOPIC = os.getenv('KAFKA_OUTPUT_TOPIC', 'news-transformed')

    consumer = KafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id=f"dev_group_{uuid.uuid4()}",
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=True
    )

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        api_version=(3, 9)
    )

    print(f"Transformer started, listening on topic '{INPUT_TOPIC}'")

    for message in consumer:
        article = message.value
        transformed = {
            "title": article.get("title"),
            "description": article.get("description")
        }
        producer.send(OUTPUT_TOPIC, value=transformed)
        producer.flush()

if __name__ == '__main__':
    time.sleep(15)
    start_transformer()
