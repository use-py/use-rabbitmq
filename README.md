# use-rabbitmq
<a href="https://github.com/use-py/use-rabbitmq/actions/workflows/test.yml?query=event%3Apush+branch%3Amain" target="_blank">
    <img src="https://github.com/use-py/use-rabbitmq/workflows/test%20suite/badge.svg?branch=main&event=push" alt="Test">
</a>
<a href="https://pypi.org/project/use-rabbitmq" target="_blank">
    <img src="https://img.shields.io/pypi/v/use-rabbitmq.svg" alt="Package version">
</a>

<a href="https://pypi.org/project/use-rabbitmq" target="_blank">
    <img src="https://img.shields.io/pypi/pyversions/use-rabbitmq.svg" alt="Supported Python versions">
</a>

A rabbitmq connector that never breaks

### example

```python
from use_rabbitmq import RabbitMQStore

rmq = RabbitMQStore()


@rmq.listener(queue_name="test")
def test_listener(message):
    print(message.body)
    message.ack()  # ack message
```
