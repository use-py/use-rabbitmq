# usepy-plugin-rabbitmq

a plugin for [usepy](https://github.com/use-py/usepy) to use rabbitmq

### example

```python
from usepy_plugin_rabbitmq import useRabbitMQ

rmq = useRabbitMQ()


@rmq.listener(queue_name="test")
def test_listener(message):
    print(message.body)
    message.ack()  # ack message
```

if you use it with [usepy](https://github.com/use-py/usepy), you can use it like this:

```python
from usepy.plugin import useRabbitMQ

rmq = useRabbitMQ()
```