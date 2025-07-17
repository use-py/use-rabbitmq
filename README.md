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

一个稳定可靠的 RabbitMQ 连接器，提供简单易用的 API 接口。

## 特性

- 自动重连机制，确保连接稳定性
- 简洁的装饰器 API，使消息监听更加直观
- 支持环境变量配置
- 线程安全设计
- 完善的错误处理和日志记录
- 支持消息确认机制

## 安装

```bash
pip install use-rabbitmq
```

或者使用 Poetry:

```bash
poetry add use-rabbitmq
```

## 快速开始

### 基本使用

```python
from use_rabbitmq import RabbitMQStore

# 创建 RabbitMQ 连接
rmq = RabbitMQStore(
    host="localhost",
    port=5672,
    username="guest",
    password="guest"
)

# 发送消息
rmq.send(queue_name="test_queue", message="Hello, RabbitMQ!")

# 使用装饰器监听队列
@rmq.listener(queue_name="test_queue")
def process_message(message):
    print(f"收到消息: {message.body}")
    message.ack()  # 确认消息已处理
```

### 使用别名

```python
from use_rabbitmq import useRabbitMQ, useRabbitListener

# 使用别名创建连接
mq = useRabbitMQ(
    host="localhost",
    port=5672,
    username="admin",
    password="admin"
)

# 方式 1: 使用实例的 listener 方法
@mq.listener(queue_name="test_queue")
def do_something(message):
    print(message.body)
    message.ack()

# 方式 2: 使用 useRabbitListener 装饰器
@useRabbitListener(mq, queue_name="another_queue")
def another_handler(message):
    print(message.body)
    message.ack()
```

## 高级用法

### 环境变量配置

可以通过环境变量配置连接参数:

```python
# 环境变量优先级: 
# RABBITMQ_HOST, RABBITMQ_PORT, RABBITMQ_USERNAME, RABBITMQ_PASSWORD

from use_rabbitmq import RabbitMQStore

# 将使用环境变量中的配置
rmq = RabbitMQStore()
```

### 队列操作

```python
# 声明队列
rmq.declare_queue("my_queue", durable=True)

# 获取队列消息数量
count = rmq.get_message_counts("my_queue")
print(f"队列中有 {count} 条消息")

# 清空队列
rmq.flush_queue("my_queue")

# 停止监听
rmq.stop_listener("my_queue")

# 关闭连接
rmq.shutdown()
```

### 消息发送选项

```python
# 发送带属性的消息
properties = {
    'content_type': 'application/json',
    'headers': {'source': 'my_app'}
}
rmq.send(
    queue_name="my_queue", 
    message='{"data": "test"}', 
    properties=properties
)
```

### 消费者配置

```python
# 配置预取数量和其他参数
@rmq.listener(queue_name="my_queue", prefetch=10, no_ack=False)
def process_batch(message):
    # 处理消息
    print(message.body)
    message.ack()
```

## 异常处理

该库内置了自动重试和重连机制:

- 连接错误自动重试
- 发送消息失败自动重试
- 消费者断线自动重连

## 贡献

欢迎提交 Issue 和 Pull Request!
