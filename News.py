import requests
import time
from datetime import datetime

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
        
        new_articles_found = False
        i = 1
        for article in articles:
            article_id = article['url']
            if article_id not in seen_articles:
                seen_articles.add(article_id)
                new_articles_found = True
                print(f"{i}. Title: {article['title']}")
                print(f"   Published At: {article['publishedAt']}")
                print(f"   Author: {article['author']}")
                print(f"   Description: {article['description']}")
                print(f"   URL: {article['url']}\n")
                i += 1

        if not new_articles_found:
            print("No new articles found.")
    else:
        print(f"Failed to fetch news. Status Code: {response.status_code}")
        print(response.json())

if __name__ == '__main__':
    api_key = 'aec54d822f854e34ab334df91c5b379d'
    keyword = input("Enter the keyword to search news for: ")
    
    while True:
        get_news(api_key, keyword)
        print("Waiting for the next check...\n")
        time.sleep(60)  
