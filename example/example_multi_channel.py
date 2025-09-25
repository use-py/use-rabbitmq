#!/usr/bin/env python3
"""
多channel管理示例

这个示例展示了如何使用多个channel来处理不同的队列操作，
提高并发性能和资源利用率。
"""

import logging
import threading
import time
from use_rabbitmq import RabbitMQStore

logging.basicConfig(level=logging.INFO)

def test_multi_channel_basic():
    """基本的多channel测试"""
    print("=== 基本多channel测试 ===")
    
    with RabbitMQStore(
        host="localhost",
        port=5672,
        username="guest",
        password="guest"
    ) as mq:
        # 创建多个channel
        channel1_id = mq.create_channel()
        channel2_id = mq.create_channel()
        
        print(f"创建了两个channel: {channel1_id[:8]}..., {channel2_id[:8]}...")
        
        # 使用不同的channel声明队列
        mq.declare_queue("queue1", channel_id=channel1_id)
        mq.declare_queue("queue2", channel_id=channel2_id)
        
        # 使用不同的channel发送消息
        mq.send("queue1", "Message to queue1 via channel1", channel_id=channel1_id)
        mq.send("queue2", "Message to queue2 via channel2", channel_id=channel2_id)
        
        # 检查消息数量
        count1 = mq.get_message_counts("queue1", channel_id=channel1_id)
        count2 = mq.get_message_counts("queue2", channel_id=channel2_id)
        
        print(f"Queue1 消息数量: {count1}")
        print(f"Queue2 消息数量: {count2}")
        
        # 列出所有channel状态
        channels = mq.list_channels()
        print(f"所有channel状态: {channels}")
        
        # 清理
        mq.flush_queue("queue1", channel_id=channel1_id)
        mq.flush_queue("queue2", channel_id=channel2_id)
        
        # 关闭channel
        mq.close_channel(channel1_id)
        mq.close_channel(channel2_id)
        
        print("多channel基本测试完成")

def test_backward_compatibility():
    """测试向后兼容性"""
    print("\n=== 向后兼容性测试 ===")
    
    with RabbitMQStore(
        host="localhost",
        port=5672,
        username="guest",
        password="guest"
    ) as mq:
        # 使用原有的API（不指定channel_id）
        mq.declare_queue("test_queue")
        mq.send("test_queue", "Backward compatibility test message")
        
        count = mq.get_message_counts("test_queue")
        print(f"使用默认channel发送消息，队列消息数量: {count}")
        
        # 清理
        mq.flush_queue("test_queue")
        
        print("向后兼容性测试通过")

def test_concurrent_channels():
    """测试并发使用多个channel"""
    print("\n=== 并发channel测试 ===")
    
    def worker(mq, worker_id, channel_id):
        """工作线程函数"""
        queue_name = f"worker_queue_{worker_id}"
        
        # 声明队列
        mq.declare_queue(queue_name, channel_id=channel_id)
        
        # 发送多条消息
        for i in range(5):
            message = f"Worker {worker_id} - Message {i}"
            mq.send(queue_name, message, channel_id=channel_id)
            time.sleep(0.1)  # 模拟处理时间
        
        # 检查消息数量
        count = mq.get_message_counts(queue_name, channel_id=channel_id)
        print(f"Worker {worker_id} (Channel {channel_id[:8]}...) 发送了 {count} 条消息")
        
        # 清理
        mq.flush_queue(queue_name, channel_id=channel_id)
    
    with RabbitMQStore(
        host="localhost",
        port=5672,
        username="guest",
        password="guest"
    ) as mq:
        # 创建多个channel
        channels = []
        threads = []
        
        for i in range(3):
            channel_id = mq.create_channel()
            channels.append(channel_id)
            
            # 创建工作线程
            thread = threading.Thread(target=worker, args=(mq, i, channel_id))
            threads.append(thread)
            thread.start()
        
        # 等待所有线程完成
        for thread in threads:
            thread.join()
        
        # 关闭所有channel
        for channel_id in channels:
            mq.close_channel(channel_id)
        
        print("并发channel测试完成")

def test_channel_recovery():
    """测试channel恢复机制"""
    print("\n=== Channel恢复测试 ===")
    
    with RabbitMQStore(
        host="localhost",
        port=5672,
        username="guest",
        password="guest"
    ) as mq:
        # 创建channel
        channel_id = mq.create_channel()
        
        # 使用channel
        mq.declare_queue("recovery_test", channel_id=channel_id)
        mq.send("recovery_test", "Test message", channel_id=channel_id)
        
        print(f"Channel {channel_id[:8]}... 创建并使用成功")
        
        # 模拟channel状态检查
        channels_status = mq.list_channels()
        print(f"Channel状态: {channels_status}")
        
        # 清理
        mq.flush_queue("recovery_test", channel_id=channel_id)
        mq.close_channel(channel_id)
        
        print("Channel恢复测试完成")

if __name__ == "__main__":
    print("RabbitMQ 多Channel管理示例")
    print("=" * 50)
    
    try:
        test_multi_channel_basic()
        test_backward_compatibility()
        test_concurrent_channels()
        test_channel_recovery()
        
        print("\n" + "=" * 50)
        print("所有多channel测试完成！")
        print("新功能特性:")
        print("1. 支持创建多个channel")
        print("2. 每个操作可以指定使用的channel")
        print("3. 保持向后兼容性")
        print("4. 支持并发使用多个channel")
        print("5. 自动管理channel生命周期")
        
    except Exception as e:
        print(f"测试运行出错: {e}")
        import traceback
        traceback.print_exc()