import logging
import time
from use_rabbitmq import RabbitConnectionFactory, get_connection_manager

logging.basicConfig(level=logging.INFO)

# 方式1: 使用RabbitConnectionFactory的装饰器
factory = RabbitConnectionFactory(
    client_name="MultiChannelApp",
    default_config={
        "host": "localhost",
        "port": 5672,
        "username": "admin",
        "password": "admin",
    }
)

# 在默认连接的不同channel上监听不同队列
@factory.listener(queue_name="orders", channel_name="order_consumer", connection_name="default")
def handle_orders(message):
    print(f"[Order Consumer] Received order: {message.body}")
    message.ack()

@factory.listener(queue_name="notifications", channel_name="notification_consumer", connection_name="default")
def handle_notifications(message):
    print(f"[Notification Consumer] Received notification: {message.body}")
    message.ack()

@factory.listener(queue_name="logs", channel_name="log_consumer", connection_name="default")
def handle_logs(message):
    print(f"[Log Consumer] Received log: {message.body}")
    message.ack()

# 方式2: 使用ConnectionManager的装饰器
conn_manager = get_connection_manager("app_connection", 
                                    host="localhost",
                                    port=5672,
                                    username="admin",
                                    password="admin")

@conn_manager.listener(queue_name="tasks", channel_name="task_consumer")
def handle_tasks(message):
    print(f"[Task Consumer] Processing task: {message.body}")
    # 模拟任务处理
    time.sleep(1)
    message.ack()

@conn_manager.listener(queue_name="events", channel_name="event_consumer")
def handle_events(message):
    print(f"[Event Consumer] Processing event: {message.body}")
    message.ack()

if __name__ == "__main__":
    print("Starting multi-channel consumers...")
    print("Factory connections:", factory.list_connections())
    print("Connection manager channels:", conn_manager.list_channels())
    
    # 发送一些测试消息
    try:
        # 使用publisher channel发送消息
        publisher_channel = conn_manager.create_channel("publisher")
        
        # 声明队列
        for queue in ["orders", "notifications", "logs", "tasks", "events"]:
            publisher_channel.queue.declare(queue, durable=True)
        
        # 发送测试消息
        test_messages = {
            "orders": "New order #12345",
            "notifications": "User login notification",
            "logs": "Application started",
            "tasks": "Process payment for order #12345",
            "events": "User registered event"
        }
        
        for queue, message in test_messages.items():
            publisher_channel.basic.publish(
                body=message,
                routing_key=queue,
                properties={'delivery_mode': 2}  # 持久化消息
            )
            print(f"Published to {queue}: {message}")
        
        # 让消费者运行一段时间
        print("\nConsumers are running. Press Ctrl+C to stop...")
        time.sleep(3000)
        
    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        # 清理资源
        factory.shutdown_all()
        conn_manager.shutdown()
        print("All connections closed.")
