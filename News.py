import requests
import time
import os
import json
from kafka import KafkaProducer

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'news-articles')

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    api_version=(3, 9)
)

# Store seen article URLs
seen_articles = set()

def get_news(api_key, keyword, language='en', page_size=100):
    url = 'https://newsapi.org/v2/everything'

    parameters = {
        'q': keyword,
        'language': language,
        'sortBy': 'publishedAt',
        'pageSize': page_size,
        'apiKey': api_key
    }
    
    response = requests.get(url, params=parameters)

    if response.status_code == 200:
        articles = response.json().get('articles', [])

        if not articles:
            print("No articles found for the keyword.")
            return
        
        for article in articles:
            article_id = article['url']
            if article_id not in seen_articles:
                seen_articles.add(article_id)
                payload = {
                    'title': article['title'],
                    'publishedAt': article['publishedAt'],
                    'author': article['author'],
                    'description': article['description'],
                    'url': article['url'],
                }

                producer.send(KAFKA_TOPIC, value=payload)
        
        producer.flush()
        
    else:
        print(f"Failed to fetch news. Status Code: {response.status_code}")
        print(response.json())

def load_keywords():
    try:
        f = open('keywords.json', 'r')
        data = json.load(f)
        return [k.strip() for k in data.get('keywords', []) if k.strip()]
    except FileNotFoundError:
        print("No keywords file found.")
        return []
    except json.JSONDecodeError:
        print("Error decoding JSON from keywords file. Please check the format.")
        return []

if __name__ == '__main__':
    api_key = os.getenv('NEWS_API_KEY')
    keywords = load_keywords()
    
    if keywords:
        
        for keyword in keywords:
            get_news(api_key, keyword)
    
    while True:
        
        for keyword in keywords:
            get_news(api_key, keyword) 
            
        time.sleep(60)
