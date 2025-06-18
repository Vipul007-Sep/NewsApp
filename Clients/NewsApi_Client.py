from Clients.Base_Client import NewsClient

class NewsApi_Client(NewsClient):
    async def fetch(self, session, keyword, language, page_size):
        params = {
            "q": keyword,
            "language": language,
            "sortBy": "publishedAt",
            "pageSize": page_size,
            "apiKey": self.source["apiKey"]
        }

        try:
            async with session.get(self.source["url"], params=params, timeout=10) as response:
                if response.status == 200:
                    data = await response.json()
                    return data.get("articles", [])
                else:
                    print(f"[ERROR] {self.source['name']} failed with status {response.status}")
        except Exception as e:
            print(f"[EXCEPTION] {self.source['name']} threw an error: {e}")
        return []
