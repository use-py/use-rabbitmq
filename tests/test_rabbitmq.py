import time

import pytest

from use_rabbitmq import useRabbitMQ


@pytest.fixture
def rabbitmq():
    return useRabbitMQ(host="localhost", port=5672, username="admin", password="admin")


def test_rabbitmq_connection(rabbitmq):
    assert rabbitmq.connection.is_open is True


def test_rabbitmq_channel(rabbitmq):
    assert rabbitmq.channel.is_open is True


def test_send(rabbitmq):
    queue_name = "test-q"
    rabbitmq.declare_queue(queue_name)
    rabbitmq.flush_queue(queue_name)
    assert rabbitmq.send(queue_name=queue_name, message="123") == "123"


def test_get_message_counts(rabbitmq):
    queue_name = "test-q2"
    rabbitmq.declare_queue(queue_name)
    rabbitmq.flush_queue(queue_name)
    assert rabbitmq.send(queue_name=queue_name, message="456") == "456"
    time.sleep(.1)
    assert rabbitmq.get_message_counts(queue_name) == 1


def test_flush_queue(rabbitmq):
    queue_name = "test-q3"
    rabbitmq.declare_queue(queue_name)
    rabbitmq.flush_queue(queue_name)
    assert rabbitmq.send(queue_name=queue_name, message="789") == "789"
    time.sleep(.1)
    assert rabbitmq.get_message_counts(queue_name) == 1
    rabbitmq.flush_queue(queue_name)
    time.sleep(.1)
    assert rabbitmq.get_message_counts(queue_name) == 0


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
    _, __, body = rabbitmq.channel.basic_get("test-q2")
    assert body == b"456"


def test_useRabbitListener(rabbitmq):
    queue_name = "test_queue"
    rabbitmq.declare_queue(queue_name)
    assert rabbitmq.send(queue_name=queue_name, message="789") == "789"

    @rabbitmq.listener(queue_name=queue_name)
    def callback(channel, deliver, properties, body):
        assert body == b"789"
        channel.basic_ack(deliver.delivery_tag)
        rabbitmq.stop_listener(queue_name)
