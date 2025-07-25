#!/usr/bin/env python3
"""
使用上下文管理器的RabbitMQ示例

这个示例展示了如何使用上下文管理器模式来确保RabbitMQ连接被正确关闭，
避免连接泄漏问题。
"""

import logging
from use_rabbitmq import RabbitMQStore

logging.basicConfig(level=logging.INFO)

def example_with_context_manager():
    """使用上下文管理器的示例"""
    print("=== 使用上下文管理器 ===")
    
    # 推荐的使用方式：使用上下文管理器
    with RabbitMQStore(
        host="localhost",
        port=5672,
        username="guest",
        password="guest"
    ) as mq:
        # 声明队列
        mq.declare_queue("test_queue", durable=True)
        
        # 发送消息
        message = "Hello from context manager!"
        mq.send("test_queue", message)
        print(f"发送消息: {message}")
        
        # 获取消息数量
        count = mq.get_message_counts("test_queue")
        print(f"队列中的消息数量: {count}")
        
        # 清空队列
        mq.flush_queue("test_queue")
        print("队列已清空")
    
    # 连接会在这里自动关闭
    print("连接已自动关闭")

def example_manual_cleanup():
    """手动清理的示例"""
    print("\n=== 手动清理资源 ===")
    
    mq = RabbitMQStore(
        host="localhost",
        port=5672,
        username="guest",
        password="guest"
    )
    
    try:
        # 声明队列
        mq.declare_queue("test_queue2", durable=True)
        
        # 发送消息
        message = "Hello from manual cleanup!"
        mq.send("test_queue2", message)
        print(f"发送消息: {message}")
        
        # 获取消息数量
        count = mq.get_message_counts("test_queue2")
        print(f"队列中的消息数量: {count}")
        
    except Exception as e:
        print(f"操作失败: {e}")
    finally:
        # 手动清理资源
        mq.shutdown()
        print("连接已手动关闭")

def example_multiple_operations():
    """多个操作的示例"""
    print("\n=== 多个操作示例 ===")
    
    with RabbitMQStore(
        host="localhost",
        port=5672,
        username="guest",
        password="guest"
    ) as mq:
        # 创建多个队列
        queues = ["queue1", "queue2", "queue3"]
        
        for queue_name in queues:
            mq.declare_queue(queue_name, durable=True)
            
            # 发送多条消息
            for i in range(3):
                message = f"Message {i+1} to {queue_name}"
                mq.send(queue_name, message)
                print(f"发送到 {queue_name}: {message}")
            
            # 检查消息数量
            count = mq.get_message_counts(queue_name)
            print(f"{queue_name} 中有 {count} 条消息")
            
            # 清空队列
            mq.flush_queue(queue_name)
            print(f"{queue_name} 已清空")
    
    print("所有操作完成，连接已自动关闭")

def example_error_handling():
    """错误处理示例"""
    print("\n=== 错误处理示例 ===")
    
    try:
        with RabbitMQStore(
            host="localhost",
            port=5672,
            username="guest",
            password="guest"
        ) as mq:
            # 正常操作
            mq.declare_queue("error_test_queue", durable=True)
            mq.send("error_test_queue", "Test message")
            
            # 模拟错误（这里故意引发异常）
            # raise ValueError("模拟的错误")
            
            print("操作成功完成")
            
    except Exception as e:
        print(f"捕获到异常: {e}")
        print("即使出现异常，连接也会被正确关闭")

if __name__ == "__main__":
    print("RabbitMQ 上下文管理器示例")
    print("=" * 50)
    
    try:
        example_with_context_manager()
        example_manual_cleanup()
        example_multiple_operations()
        example_error_handling()
        
        print("\n" + "=" * 50)
        print("所有示例完成！")
        print("建议使用上下文管理器模式以确保资源正确清理")
        while True:
            pass
        
    except Exception as e:
        print(f"示例运行出错: {e}")
