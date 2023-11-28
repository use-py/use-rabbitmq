import logging

from use_rabbitmq import useRabbitMQ

logging.basicConfig(level=logging.INFO)

mq = useRabbitMQ(
    host="localhost",
    port=5672,
    username="admin",
    password="admin",
)


# @useRabbitListener(mq, queue_name="test_queue")
# or
@mq.listener(queue_name="test_queue")
def do_something(message):
    print(message.body)
    message.ack()
