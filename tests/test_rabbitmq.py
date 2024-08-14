import os

import pytest

from use_rabbitmq import useRabbitMQ, RabbitListener

os.environ.setdefault("RABBITMQ_PASSWORD", "admin")


@pytest.fixture
def rabbitmq():
    return useRabbitMQ(host="localhost", port=5672, username="admin")


def test_rabbitmq_connection(rabbitmq):
    assert rabbitmq.connection.is_open is True


def test_rabbitmq_channel(rabbitmq):
    assert rabbitmq.channel.is_open is True


def test_send(rabbitmq):
    rabbitmq.declare_queue("test-q")
    assert rabbitmq.send(queue_name="test-q", message="123") == "123"


def test_get_message_counts(rabbitmq):
    queue_name = "test-q2"
    rabbitmq.declare_queue(queue_name)
    rabbitmq.flush_queue(queue_name)
    assert rabbitmq.send(queue_name=queue_name, message="456") == "456"
    assert rabbitmq.get_message_counts(queue_name) == 1


def test_flush_queue(rabbitmq):
    rabbitmq.declare_queue("test-q3")
    assert rabbitmq.send(queue_name="test-q3", message="789") == "789"
    assert rabbitmq.get_message_counts("test-q3") == 1
    rabbitmq.flush_queue("test-q3")
    assert rabbitmq.get_message_counts("test-q3") == 0


def test_close_connection(rabbitmq):
    rabbitmq.connection.close()
    connection = rabbitmq._connection
    assert connection.is_open is False
    assert rabbitmq.connection.is_open is True


def test_close_channel(rabbitmq):
    rabbitmq.channel.close()
    channel = rabbitmq._channel
    assert channel.is_open is False
    assert rabbitmq.channel.is_open is True


def test_get_message(rabbitmq):
    message = rabbitmq.channel.basic.get("test-q2")
    assert message.body == "456"


def test_useRabbitListener(rabbitmq):
    queue_name = "test_queue"
    assert rabbitmq.send(queue_name=queue_name, message="7890") == "7890"

    @RabbitListener(rabbitmq, queue_name=queue_name)
    def callback(message):
        assert message.body == "7890"
        rabbitmq.stop_listener(queue_name)
