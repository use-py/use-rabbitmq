from use_rabbitmq import useRabbitMQ

rmq = useRabbitMQ(
    host="localhost",
    port=5672,
    username="admin",
    password="admin",
    confirm_delivery=True,
)

DEAD_EXCHANGE_NAME = "clue.exchange"
DEAD_ROUTING_KEY = "clue.routing.key"
arguments = {
    "x-dead-letter-exchange": DEAD_EXCHANGE_NAME,
    "x-dead-letter-routing-key": DEAD_ROUTING_KEY,
    # "x-message-ttl": 10 * 1000
}
print(rmq.declare_queue("queue_name", arguments=arguments))


def callback(channel, deliver, properties, message):
    print(message)
    channel.basic_ack(deliver.delivery_tag)


rmq.start_consuming("queue_name", callback)
