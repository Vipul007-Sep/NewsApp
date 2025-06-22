import pytest
from unittest.mock import patch, MagicMock, AsyncMock

# Sample mock data
sample_sources = [
    {
        "name": "NewsApi1",
        "url": "https://newsapi.org/v2/everything",
        "enabled": True,
        "apiKey": "dummy-api-key"
    }
]

sample_keywords = ["ai"]

sample_articles = [
    {
        "title": "Test Title",
        "publishedAt": "2025-06-17T12:00:00Z",
        "author": "Author Name",
        "description": "Some news description.",
        "url": "http://example.com/article-123"
    }
]

@pytest.mark.asyncio
async def test_get_news_logic():
    import News

    with patch.object(News, "load_sources", return_value=sample_sources), \
         patch.object(News, "load_keywords", return_value=sample_keywords), \
         patch("News.create_client") as mock_create_client, \
         patch("News.producer") as mock_producer:

        mock_client = MagicMock()
        mock_client.fetch = AsyncMock(return_value=sample_articles)
        mock_create_client.return_value = mock_client

        await News.get_news("ai")

        # Assertions
        assert mock_client.fetch.call_count == 1
        assert mock_producer.send.call_count == len(sample_articles)
        assert mock_producer.flush.called is True

        for call in mock_producer.send.call_args_list:
            topic = call[0][0]
            value = call[1]["value"]
            assert topic == "news-articles"
            assert isinstance(value["title"], str) and value["title"]
            assert isinstance(value["publishedAt"], str) and value["publishedAt"]
            assert isinstance(value["author"], str) and value["author"]
            assert isinstance(value["description"], str) and value["description"]
            assert isinstance(value["url"], str) and value["url"]

def test_load_keywords_empty():
    import News
    with patch.object(News, "load_keywords", return_value=[]):
        result = News.load_keywords()
        assert result == []

def test_load_sources_empty():
    import News
    with patch.object(News, "load_sources", return_value=[]):
        result = News.load_sources()
        assert result == []
