import pytest
from unittest.mock import patch, MagicMock
import News_Consumer

sample_article = {
    "title": "Test Title",
    "description": "Test Description"
}

# 1. Test: PostgreSQL connection success
@patch("psycopg2.connect")
def test_wait_for_postgres_success(mock_connect):
    mock_connect.return_value = MagicMock()
    conn = News_Consumer.wait_for_postgres(max_retries=1, delay=0)
    assert conn is not None

# 2. Test: Table creation success
@patch("News_Consumer.wait_for_postgres")
def test_setup_database_success(mock_wait):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_wait.return_value = mock_conn

    News_Consumer.conn = None
    News_Consumer.cursor = None

    News_Consumer.setup_database()

    mock_cursor.execute.assert_called_once()
    mock_conn.commit.assert_called_once()

# 3. Test: Insert article to DB
def test_insert_article_to_db_success():
    News_Consumer.conn = MagicMock()
    News_Consumer.cursor = MagicMock()

    News_Consumer.insert_article_to_db("title", "desc")
    News_Consumer.cursor.execute.assert_called_once()
    News_Consumer.conn.commit.assert_called_once()

# 4. Test: Process message in print mode
def test_process_message_print(capfd):
    News_Consumer.CONSUMER_MODE = "print"
    News_Consumer.process_message(sample_article)
    out, _ = capfd.readouterr()
    assert "Test Title" in out
    assert "Test Description" in out

# 5. Test: Kafka consumer reads and processes one message
@patch("News_Consumer.KafkaConsumer")
def test_start_kafka_consumer(mock_kafka):
    mock_msg = MagicMock()
    mock_msg.value = sample_article
    mock_consumer = MagicMock()
    mock_consumer.__iter__.return_value = [mock_msg]
    mock_kafka.return_value = mock_consumer

    with patch("News_Consumer.process_message") as mock_process:
        News_Consumer.start_kafka_consumer()
        mock_process.assert_called_once_with(sample_article)
