import pytest
from unittest.mock import patch, AsyncMock, MagicMock
import News  


sample_sources = [
    {
        "name": "NewsApi1",
        "url": "https://newsapi.org/v2/everything",
        "enabled": True,
        "apiKey": "dummy-api-key"
    }
]

sample_keywords = ["technology"]

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
@patch.object(News, "load_sources", return_value=sample_sources)
@patch.object(News, "load_keywords", return_value=sample_keywords)
@patch("News.create_client")
@patch("News.producer")
async def test_get_news_with_valid_sources_and_articles(
    mock_producer, mock_create_client, mock_load_keywords, mock_load_sources
):
    expected_articles = sample_articles
    mock_client = MagicMock()
    mock_client.fetch = AsyncMock(return_value=sample_articles)
    mock_create_client.return_value = mock_client

    sent_payloads = []

    def fake_send(topic, value):
        sent_payloads.append(value)

    mock_producer.send.side_effect = fake_send

    await News.get_news("technology")

    print("\nExpected Articles:")
    for a in expected_articles:
        print(a)
    print("\nSent to Kafka:")
    for p in sent_payloads:
        print(p)


    assert mock_producer.send.called
    assert mock_producer.flush.called
    mock_client.fetch.assert_called_once()


@patch.object(News, "load_keywords", return_value=[])
def test_load_keywords_empty(mock_load_keywords):
    result = News.load_keywords()
    assert result == []


@patch.object(News, "load_sources", return_value=[])
def test_load_sources_empty(mock_load_sources):
    result = News.load_sources()
    assert result == []
