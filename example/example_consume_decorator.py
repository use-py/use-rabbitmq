import logging
import time

from use_rabbitmq import useRabbitMQ, useRabbitListener

logging.basicConfig(level=logging.INFO)

mq = useRabbitMQ(
    host="localhost",
    port=5672,
    username="admin",
    password="admin",
)


def stop_listener_when_timeout(client):
    if time.time() - client.last_message_time > 10:
        print("停止监听")
        return True


@useRabbitListener(
    mq, queue_name="test_queue", stop_listener=stop_listener_when_timeout
)
def do_something(message):
    print(message.body)
    message.ack()
