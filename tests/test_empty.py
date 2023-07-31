import pytest

from usepy_plugin_rabbitmq import useRabbitMQ


@pytest.fixture
def rabbitmq():
    return useRabbitMQ(
        username="miclon",
        password="miclon"
    )


def test_rabbitmq_connection(rabbitmq):
    assert rabbitmq.connection.is_open is True


def test_rabbitmq_channel(rabbitmq):
    assert rabbitmq.channel.is_open is True


def test_send(rabbitmq):
    assert rabbitmq.send(
        queue_name="test-q",
        message="123"
    ) == "123"


def test_get_message_counts(rabbitmq):
    rabbitmq.flush_queue("test-q2")
    assert rabbitmq.send(
        queue_name="test-q2",
        message="456"
    ) == "456"
    assert rabbitmq.get_message_counts("test-q2") == 1


def test_flush_queue(rabbitmq):
    assert rabbitmq.send(
        queue_name="test-q3",
        message="789"
    ) == "789"
    assert rabbitmq.get_message_counts("test-q3") == 1
    rabbitmq.flush_queue("test-q3")
    assert rabbitmq.get_message_counts("test-q3") == 0


def test_close_connection(rabbitmq):
    rabbitmq.connection.close()
    connection = getattr(rabbitmq.state, "connection", None)
    assert connection.is_open is False
    assert rabbitmq.connection.is_open is True


def test_close_channel(rabbitmq):
    rabbitmq.channel.close()
    channel = getattr(rabbitmq.state, "channel", None)
    assert channel.is_open is False
    assert rabbitmq.channel.is_open is True


def test_get_message(rabbitmq):
    message = rabbitmq.channel.basic.get("test-q2")
    assert message.body == "456"
