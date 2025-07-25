#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
RabbitConnectionFactory 使用示例

本示例展示了如何使用 RabbitConnectionFactory 来管理 RabbitMQ 连接，
特别是如何设置 client_name 和在一个连接上创建多个 channel。

特性演示:
- 设置 client_name（类似Java客户端）
- 一个连接创建多个 channel
- 连接和 channel 的管理
- 线程安全操作
"""

import time
import threading
from use_rabbitmq import (
    RabbitConnectionFactory,
    rabbitConnectionFactory,
    get_default_factory,
    create_rabbit_connection,
    get_connection_manager,
    ConnectionManager
)


def example_client_name():
    """演示设置client_name功能"""
    print("=== Client Name 设置示例 ===")
    
    # 创建带有自定义client_name的工厂
    factory = RabbitConnectionFactory(client_name="MyPythonApp")
    
    # 获取连接管理器
    conn_manager = factory.get_connection_manager("main")
    
    print(f"连接已创建，client_name: MyPythonApp#main")
    print("你可以在RabbitMQ管理界面的Connections页面看到这个名称")

    
    # 创建一个channel测试连接
    channel = conn_manager.create_channel("test")
    channel = conn_manager.create_channel("test2")
    channel = conn_manager.create_channel("test4")
    print(f"Channel创建成功: {channel}")
    
    # 清理
    factory.shutdown_all()
    print("连接已关闭")


def example_multiple_channels():
    """演示一个连接创建多个channel"""
    print("\n=== 多Channel管理示例 ===")
    
    factory = RabbitConnectionFactory(client_name="MultiChannelApp")
    conn_manager = factory.get_connection_manager("production")
    
    # 创建多个不同用途的channel
    publisher_channel = conn_manager.create_channel("publisher", confirm_delivery=True)
    consumer_channel = conn_manager.create_channel("consumer", confirm_delivery=False)
    admin_channel = conn_manager.create_channel("admin", confirm_delivery=False)
    
    print(f"已创建的channels: {conn_manager.list_channels()}")
    
    # 使用不同的channel进行操作
    try:
        # 声明队列
        publisher_channel.queue.declare("test_queue", durable=True)
        
        # 发送消息
        publisher_channel.basic.publish(
            "Hello from publisher channel!",
            "test_queue"
        )
        print("消息已通过publisher channel发送")
        
        # 获取队列信息
        queue_info = admin_channel.queue.declare("test_queue", passive=True)
        print(f"队列信息: {queue_info}")
        
    except Exception as e:
        print(f"操作出错: {e}")
    
    # 关闭特定channel
    conn_manager.close_channel("admin")
    print(f"关闭admin channel后的channels: {conn_manager.list_channels()}")
    
    # 清理所有资源
    factory.shutdown_all()
    print("所有连接和channel已关闭")


def example_connection_reuse():
    """演示连接复用"""
    print("\n=== 连接复用示例 ===")
    
    factory = RabbitConnectionFactory(client_name="ReuseApp")
    
    # 获取同名连接管理器（应该返回同一个实例）
    manager1 = factory.get_connection_manager("shared")
    manager2 = factory.get_connection_manager("shared")
    
    print(f"manager1 和 manager2 是同一个实例: {manager1 is manager2}")
    
    # 在同一个连接上创建不同的channel
    channel1 = manager1.create_channel("worker1")
    channel2 = manager2.create_channel("worker2")
    
    print(f"两个channel都来自同一个连接管理器: {manager1 is manager2}")
    print(f"当前channels: {manager1.list_channels()}")
    
    factory.shutdown_all()


def example_thread_safety():
    """演示线程安全操作"""
    print("\n=== 线程安全示例 ===")
    
    factory = RabbitConnectionFactory(client_name="ThreadSafeApp")
    conn_manager = factory.get_connection_manager("threaded")
    
    def worker(worker_id):
        """工作线程函数"""
        try:
            # 每个线程创建自己的channel
            channel = conn_manager.create_channel(f"worker_{worker_id}")
            
            # 声明队列
            queue_name = f"queue_{worker_id}"
            channel.queue.declare(queue_name, durable=True)
            
            # 发送消息
            for i in range(3):
                message = f"Message {i} from worker {worker_id}"
                channel.basic.publish(message, queue_name)
            
            print(f"Worker {worker_id} 完成任务")
            
        except Exception as e:
            print(f"Worker {worker_id} 出错: {e}")
    
    # 创建多个线程
    threads = []
    for i in range(3):
        thread = threading.Thread(target=worker, args=(i,))
        threads.append(thread)
        thread.start()
    
    # 等待所有线程完成
    for thread in threads:
        thread.join()
    
    print(f"所有线程完成，当前channels: {conn_manager.list_channels()}")
    
    factory.shutdown_all()


def example_context_manager():
    """演示上下文管理器使用"""
    print("\n=== 上下文管理器示例 ===")
    
    # 工厂级别的上下文管理器
    with RabbitConnectionFactory(client_name="ContextApp") as factory:
        conn_manager = factory.get_connection_manager("context_test")
        
        # 连接管理器级别的上下文管理器
        with conn_manager as manager:
            channel1 = manager.create_channel("temp1")
            channel2 = manager.create_channel("temp2")
            
            print(f"在上下文中创建的channels: {manager.list_channels()}")
            
            # 进行一些操作
            channel1.queue.declare("temp_queue")
            channel1.basic.publish("Test message", "temp_queue")
            
        print("连接管理器上下文退出，资源已自动清理")
    
    print("工厂上下文退出，所有资源已自动清理")


def example_error_handling():
    """演示错误处理"""
    print("\n=== 错误处理示例 ===")
    
    factory = RabbitConnectionFactory(client_name="ErrorHandlingApp")
    
    try:
        # 使用错误的配置
        conn_manager = factory.get_connection_manager(
            "error_test",
            host="nonexistent_host",
            port=9999
        )
        
        # 尝试创建channel（这会触发连接）
        channel = conn_manager.create_channel("test")
        
    except Exception as e:
        print(f"预期的连接错误: {e}")
    
    # 测试已关闭的管理器
    conn_manager = factory.get_connection_manager("normal")
    conn_manager.shutdown()
    
    try:
        # 尝试在已关闭的管理器上创建channel
        channel = conn_manager.create_channel("test")
    except RuntimeError as e:
        print(f"预期的运行时错误: {e}")
    
    factory.shutdown_all()


def example_advanced_usage():
    """演示高级用法"""
    print("\n=== 高级用法示例 ===")
    
    # 使用自定义配置创建工厂
    custom_config = {
        "host": "localhost",
        "port": 5672,
        "username": "guest",
        "password": "guest",
        "heartbeat": 60,
        "virtual_host": "/"
    }
    
    factory = RabbitConnectionFactory(
        client_name="AdvancedApp",
        default_config=custom_config
    )
    
    # 创建不同环境的连接
    prod_manager = factory.get_connection_manager("production")
    dev_manager = factory.get_connection_manager(
        "development",
        virtual_host="/dev"
    )
    
    print(f"活跃连接: {factory.list_connections()}")
    
    # 为不同用途创建专门的channel
    # 生产环境
    prod_publisher = prod_manager.create_channel("publisher")
    prod_consumer = prod_manager.create_channel("consumer")
    
    # 开发环境
    dev_tester = dev_manager.create_channel("tester")
    
    print(f"生产环境channels: {prod_manager.list_channels()}")
    print(f"开发环境channels: {dev_manager.list_channels()}")
    
    # 选择性关闭
    factory.remove_connection_manager("development")
    print(f"关闭开发环境后的连接: {factory.list_connections()}")
    
    factory.shutdown_all()


if __name__ == "__main__":
    print("RabbitConnectionFactory 高级功能演示")
    print("=" * 60)
    
    try:
        example_client_name()
        example_multiple_channels()
        example_connection_reuse()
        example_thread_safety()
        example_context_manager()
        example_error_handling()
        example_advanced_usage()
        
        print("\n所有示例执行完成！")
        print("\n主要特性总结:")
        print("1. ✅ 支持设置client_name，在RabbitMQ管理界面可见")
        print("2. ✅ 一个连接可以创建多个channel")
        print("3. ✅ 连接和channel的复用管理")
        print("4. ✅ 线程安全操作")
        print("5. ✅ 上下文管理器自动资源清理")
        print("6. ✅ 完善的错误处理")
        
    except Exception as e:
        print(f"示例执行出错: {e}")
        print("请确保 RabbitMQ 服务正在运行，并且连接配置正确。")
