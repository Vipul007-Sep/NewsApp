import time
import os
import json
from kafka import KafkaProducer
import asyncio
import aiohttp

from Clients.NewsApi_Client import NewsApi_Client  

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'news-articles')

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    api_version=(3, 9)
)

seen_articles = set()

def load_sources():
    try:
        with open("sources.json", "r") as f:
            sources = json.load(f)
            return [src for src in sources if src.get("enabled", False)]
    except FileNotFoundError:
        print("[ERROR] No sources.json file found.")
        return []
    except json.JSONDecodeError:
        print("[ERROR] Failed to parse sources.json.")
        return []


def load_keywords():
    try:
        with open('keywords.json', 'r') as f:
            data = json.load(f)
            return [k.strip() for k in data.get('keywords', []) if k.strip()]
    except FileNotFoundError:
        print("No keywords file found.")
        return []
    except json.JSONDecodeError:
        print("Error decoding keywords file.")
        return []

def create_client(source):
    return NewsApi_Client(source)

async def get_news(keyword, language='en', page_size=100):
    sources = load_sources()

    async with aiohttp.ClientSession() as session:
        tasks = []

        for source in sources:

            client = create_client(source)

            async def fetch_and_send(client=client, source=source):  # capture vars in closure
                articles = await client.fetch(session, keyword, language, page_size)
                for article in articles:
                    article_id = article.get('url')
                    if article_id and article_id not in seen_articles:
                        seen_articles.add(article_id)
                        payload = {
                            'title': article.get('title'),
                            'publishedAt': article.get('publishedAt'),
                            'author': article.get('author'),
                            'description': article.get('description'),
                            'url': article_id
                        }
                        producer.send(KAFKA_TOPIC, value=payload)
                producer.flush()

            tasks.append(fetch_and_send())

        await asyncio.gather(*tasks)


if __name__ == '__main__':
    keywords = load_keywords()

    if keywords:
        for keyword in keywords:
            asyncio.run(get_news(keyword))

    while True:
        for keyword in keywords:
            asyncio.run(get_news(keyword))
        time.sleep(60)
