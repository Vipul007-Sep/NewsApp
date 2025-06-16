import time
import os
import json
from kafka import KafkaProducer
import asyncio
import aiohttp

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'news-articles')

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    api_version=(3, 9)
)

seen_articles = set()

async def get_news(api_key, keyword, language='en', page_size=100):
    sources = [
        {
            'name': 'NewsApi1',
            'enabled': os.getenv('NEWSAPI1_ENABLED', 'true').lower() == 'true',
            'url': 'https://newsapi.org/v2/everything',
            'params': {
                'q': keyword,
                'language': language,
                'sortBy': 'publishedAt',
                'pageSize': page_size,
                'apiKey': api_key
            }
        },
        {
            'name': 'NewsApi2',
            'enabled': os.getenv('NEWSAPI2_ENABLED', 'true').lower() == 'true',
            'url': 'https://newsapi.org/v2/everything',
            'params': {
                'q': keyword,
                'language': language,
                'sortBy': 'publishedAt',
                'pageSize': page_size,
                'apiKey': api_key
            }
        },
        {
            'name': 'NewsApi3',
            'enabled': os.getenv('NEWSAPI3_ENABLED', 'true').lower() == 'true',
            'url': 'https://newsapi.org/v2/everything',
            'params': {
                'q': keyword,
                'language': language,
                'sortBy': 'publishedAt',
                'pageSize': page_size,
                'apiKey': api_key
            }
        }
    ]

    async with aiohttp.ClientSession() as session:
        async def fetch_source(source):
            if not source['enabled']:
                print(f"[INFO] {source['name']} is disabled via flag.")
                return

            try:
                async with session.get(source['url'], params=source['params'], timeout=10) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        articles = data.get('articles', [])
                        if not articles:
                            print(f"[INFO] No articles from {source['name']} for keyword '{keyword}'.")
                            return

                        for article in articles:
                            article_id = article.get('url')
                            if article_id and article_id not in seen_articles:
                                seen_articles.add(article_id)
                                payload = {
                                    'source': source['name'],
                                    'title': article.get('title'),
                                    'publishedAt': article.get('publishedAt'),
                                    'author': article.get('author'),
                                    'description': article.get('description'),
                                    'url': article_id,
                                }
                                producer.send(KAFKA_TOPIC, value=payload)
                        producer.flush()
                    else:
                        print(f"[ERROR] {source['name']} failed with status {resp.status}")
            except Exception as e:
                print(f"[EXCEPTION] {source['name']} threw an error: {e}")

        await asyncio.gather(*(fetch_source(src) for src in sources), return_exceptions=True)

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

if __name__ == '__main__':
    api_key = os.getenv('NEWS_API_KEY')
    keywords = load_keywords()

    if keywords:
        for keyword in keywords:
            asyncio.run(get_news(api_key, keyword))

    while True:
        for keyword in keywords:
            asyncio.run(get_news(api_key, keyword))
        time.sleep(60)
