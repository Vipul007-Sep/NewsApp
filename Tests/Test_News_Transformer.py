import pytest
from unittest.mock import patch, MagicMock

sample_article = {
    "title": "Sample Title",
    "description": "Sample Description"
}

@patch("News_Transformer.KafkaConsumer")
@patch("News_Transformer.KafkaProducer")
def test_start_transformer(mock_producer_class, mock_consumer_class):
    # Mock KafkaConsumer
    mock_consumer = MagicMock()
    mock_message = MagicMock()
    mock_message.value = sample_article
    mock_consumer.__iter__.return_value = [mock_message]
    mock_consumer_class.return_value = mock_consumer

    # Mock KafkaProducer
    mock_producer = MagicMock()
    mock_producer_class.return_value = mock_producer

    # Import and run the function
    import News_Transformer
    News_Transformer.start_transformer()

    # Validate producer was called with transformed article
    mock_producer.send.assert_called_once_with(
        'news-transformed',
        value={'title': 'Sample Title', 'description': 'Sample Description'}
    )
    mock_producer.flush.assert_called_once()
