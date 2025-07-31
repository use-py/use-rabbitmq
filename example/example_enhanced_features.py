#!/usr/bin/env python3
"""
增强功能示例

这个示例展示了RabbitMQ库的三个核心增强功能：
1. 连接池管理
2. 客户端自定义名称支持
3. 单连接多通道支持
"""

import logging
import time
import threading
from concurrent.futures import ThreadPoolExecutor
from use_rabbitmq import RabbitMQStore

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def example_connection_pool():
    """连接池管理示例"""
    print("\n=== 连接池管理示例 ===")
    
    # 创建多个客户端实例，它们将共享连接池
    clients = []
    for i in range(3):
        client = RabbitMQStore(
            host="localhost",
            port=5672,
            username="guest",
            password="guest",
            client_name=f"pool-client-{i+1}",
            use_connection_pool=True,
            use_channel_manager=True
        )
        clients.append(client)
    
    # 使用多个客户端发送消息
    for i, client in enumerate(clients):
        with client:
            client.declare_queue("pool_test_queue", durable=True)
            client.send("pool_test_queue", f"Message from client {i+1}")
            print(f"客户端 {client.client_name} 发送消息完成")
            
            # 显示连接池统计信息
            pool_stats = client.get_connection_pool_stats()
            if pool_stats:
                print(f"连接池统计: {pool_stats}")
    
    print("连接池示例完成")

def example_client_names():
    """客户端自定义名称示例"""
    print("\n=== 客户端自定义名称示例 ===")
    
    # 创建具有自定义名称的客户端
    with RabbitMQStore(
        host="localhost",
        port=5672,
        username="guest",
        password="guest",
        client_name="my-custom-producer",
        use_connection_pool=True
    ) as producer:
        
        # 显示客户端信息
        client_info = producer.get_client_info()
        print(f"客户端信息: {client_info['client_name']}")
        print(f"连接参数: {client_info['connection_parameters']}")
        
        producer.declare_queue("named_client_queue", durable=True)
        producer.send("named_client_queue", "Hello from named client!")
        print("自定义名称客户端发送消息完成")

def example_multi_channel():
    """单连接多通道示例"""
    print("\n=== 单连接多通道示例 ===")
    
    with RabbitMQStore(
        host="localhost",
        port=5672,
        username="guest",
        password="guest",
        client_name="multi-channel-client",
        use_connection_pool=True,
        use_channel_manager=True,
        max_channels=10
    ) as mq:
        
        # 声明队列
        mq.declare_queue("multi_channel_queue", durable=True)
        
        # 使用多个通道并发发送消息
        def send_messages(thread_id):
            for i in range(5):
                # 使用上下文管理器获取通道
                with mq.get_channel() as channel:
                    message = f"Message {i+1} from thread {thread_id}"
                    channel.basic.publish(message, "multi_channel_queue")
                    print(f"线程 {thread_id} 发送: {message}")
                    time.sleep(0.1)  # 模拟处理时间
        
        # 创建多个线程并发发送
        threads = []
        for i in range(3):
            thread = threading.Thread(target=send_messages, args=(i+1,))
            threads.append(thread)
            thread.start()
        
        # 等待所有线程完成
        for thread in threads:
            thread.join()
        
        # 显示通道管理器统计信息
        channel_stats = mq.get_channel_manager_stats()
        if channel_stats:
            print(f"通道管理器统计: {channel_stats}")
        
        # 检查消息数量
        count = mq.get_message_counts("multi_channel_queue")
        print(f"队列中的消息数量: {count}")
        
        # 清空队列
        mq.flush_queue("multi_channel_queue")
        print("队列已清空")

def example_performance_comparison():
    """性能对比示例"""
    print("\n=== 性能对比示例 ===")
    
    # 测试传统模式
    print("测试传统模式...")
    start_time = time.time()
    
    with RabbitMQStore(
        host="localhost",
        port=5672,
        username="guest",
        password="guest",
        client_name="traditional-client",
        use_connection_pool=False,
        use_channel_manager=False
    ) as mq:
        mq.declare_queue("perf_test_queue", durable=True)
        for i in range(100):
            mq.send("perf_test_queue", f"Traditional message {i+1}")
    
    traditional_time = time.time() - start_time
    print(f"传统模式耗时: {traditional_time:.2f}秒")
    
    # 测试增强模式
    print("测试增强模式...")
    start_time = time.time()
    
    with RabbitMQStore(
        host="localhost",
        port=5672,
        username="guest",
        password="guest",
        client_name="enhanced-client",
        use_connection_pool=True,
        use_channel_manager=True,
        max_channels=10
    ) as mq:
        mq.declare_queue("perf_test_queue", durable=True)
        
        # 使用线程池并发发送
        def send_batch(start_idx, count):
            for i in range(count):
                with mq.get_channel() as channel:
                    message = f"Enhanced message {start_idx + i + 1}"
                    channel.basic.publish(message, "perf_test_queue")
        
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = []
            for i in range(5):
                future = executor.submit(send_batch, i * 20, 20)
                futures.append(future)
            
            # 等待所有任务完成
            for future in futures:
                future.result()
    
    enhanced_time = time.time() - start_time
    print(f"增强模式耗时: {enhanced_time:.2f}秒")
    print(f"性能提升: {((traditional_time - enhanced_time) / traditional_time * 100):.1f}%")
    
    # 清空测试队列
    with RabbitMQStore(
        host="localhost",
        port=5672,
        username="guest",
        password="guest"
    ) as mq:
        mq.flush_queue("perf_test_queue")

def example_monitoring():
    """监控和统计示例"""
    print("\n=== 监控和统计示例 ===")
    
    with RabbitMQStore(
        host="localhost",
        port=5672,
        username="guest",
        password="guest",
        client_name="monitoring-client",
        use_connection_pool=True,
        use_channel_manager=True,
        max_channels=5
    ) as mq:
        
        # 获取完整的客户端信息
        client_info = mq.get_client_info()
        print("客户端完整信息:")
        for key, value in client_info.items():
            print(f"  {key}: {value}")
        
        # 执行一些操作
        mq.declare_queue("monitoring_queue", durable=True)
        
        # 使用多个通道
        for i in range(3):
            with mq.get_channel() as channel:
                channel.basic.publish(f"Monitoring message {i+1}", "monitoring_queue")
        
        # 再次检查统计信息
        print("\n操作后的统计信息:")
        pool_stats = mq.get_connection_pool_stats()
        channel_stats = mq.get_channel_manager_stats()
        
        if pool_stats:
            print(f"连接池统计: {pool_stats}")
        if channel_stats:
            print(f"通道管理器统计: {channel_stats}")
        
        # 清空队列
        mq.flush_queue("monitoring_queue")

if __name__ == "__main__":
    print("RabbitMQ 增强功能示例")
    print("=" * 50)
    
    try:
        example_client_names()
        example_connection_pool()
        example_multi_channel()
        example_performance_comparison()
        example_monitoring()
        while True:
            pass
        
        print("\n" + "=" * 50)
        print("所有增强功能示例完成！")
        print("\n主要增强功能:")
        print("1. 连接池管理 - 提高连接复用率和性能")
        print("2. 客户端自定义名称 - 便于监控和调试")
        print("3. 单连接多通道 - 提高并发性能和资源利用率")
        
    except Exception as e:
        print(f"示例运行出错: {e}")
        import traceback
        traceback.print_exc()
