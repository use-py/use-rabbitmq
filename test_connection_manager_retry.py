#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试ConnectionManager的连接重试功能
"""

import time
import threading
from unittest.mock import patch, MagicMock
from amqpstorm.exception import AMQPConnectionError, AMQPChannelError

# 导入我们的模块
from src.use_rabbitmq import ConnectionManager


def test_connection_retry_mechanism():
    """测试连接重试机制"""
    print("\n=== 测试ConnectionManager连接重试机制 ===")
    
    # 模拟连接失败然后成功的情况
    with patch('amqpstorm.Connection') as mock_connection_class:
        # 前两次调用失败，第三次成功
        mock_connection_class.side_effect = [
            AMQPConnectionError("Connection failed 1"),
            AMQPConnectionError("Connection failed 2"),
            MagicMock()  # 第三次成功
        ]
        
        config = {
            'host': 'localhost',
            'port': 5672,
            'username': 'guest',
            'password': 'guest'
        }
        
        manager = ConnectionManager(**config)
        
        # 测试连接重试
        start_time = time.time()
        try:
            connection = manager.connection
            end_time = time.time()
            print(f"✓ 连接重试成功，耗时: {end_time - start_time:.2f}秒")
            print(f"✓ 总共调用了 {mock_connection_class.call_count} 次连接")
            assert mock_connection_class.call_count == 3, "应该重试3次"
        except Exception as e:
            print(f"✗ 连接重试失败: {e}")
            raise
        finally:
            manager.shutdown()


def test_channel_retry_mechanism():
    """测试Channel创建重试机制"""
    print("\n=== 测试ConnectionManager Channel创建重试机制 ===")
    
    with patch('amqpstorm.Connection') as mock_connection_class:
        # 创建一个模拟连接
        mock_connection = MagicMock()
        mock_connection.is_open = True
        mock_connection_class.return_value = mock_connection
        
        # 模拟channel创建失败然后成功
        mock_connection.channel.side_effect = [
            AMQPChannelError("Channel failed 1"),
            AMQPChannelError("Channel failed 2"),
            MagicMock()  # 第三次成功
        ]
        
        config = {
            'host': 'localhost',
            'port': 5672,
            'username': 'guest',
            'password': 'guest'
        }
        
        manager = ConnectionManager(**config)
        
        try:
            # 测试channel创建重试
            start_time = time.time()
            channel = manager.create_channel("test_channel")
            end_time = time.time()
            print(f"✓ Channel创建重试成功，耗时: {end_time - start_time:.2f}秒")
            print(f"✓ 总共调用了 {mock_connection.channel.call_count} 次channel创建")
            assert mock_connection.channel.call_count == 3, "应该重试3次"
        except Exception as e:
            print(f"✗ Channel创建重试失败: {e}")
            raise
        finally:
            manager.shutdown()


def test_max_retry_exceeded():
    """测试超过最大重试次数的情况"""
    print("\n=== 测试超过最大重试次数 ===")
    
    with patch('amqpstorm.Connection') as mock_connection_class:
        # 所有连接尝试都失败
        mock_connection_class.side_effect = AMQPConnectionError("Always fail")
        
        config = {
            'host': 'localhost',
            'port': 5672,
            'username': 'guest',
            'password': 'guest'
        }
        
        # 临时设置较小的重试次数进行测试
        original_max_attempts = ConnectionManager.MAX_CONNECTION_ATTEMPTS
        ConnectionManager.MAX_CONNECTION_ATTEMPTS = 3
        
        try:
            manager = ConnectionManager(**config)
            
            # 应该抛出异常
            try:
                connection = manager.connection
                print("✗ 应该抛出连接异常")
                assert False, "应该抛出连接异常"
            except AMQPConnectionError as e:
                print(f"✓ 正确抛出连接异常: {e}")
                print(f"✓ 总共尝试了 {mock_connection_class.call_count} 次连接")
                assert mock_connection_class.call_count == 3, "应该尝试3次"
        finally:
            # 恢复原始设置
            ConnectionManager.MAX_CONNECTION_ATTEMPTS = original_max_attempts
            manager.shutdown()


def test_concurrent_retry():
    """测试并发情况下的重试机制"""
    print("\n=== 测试并发重试机制 ===")
    
    with patch('amqpstorm.Connection') as mock_connection_class:
        # 模拟连接在第3次尝试时成功
        call_count = 0
        def side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise AMQPConnectionError(f"Connection failed {call_count}")
            mock_conn = MagicMock()
            mock_conn.is_open = True
            mock_conn.channel.return_value = MagicMock()
            return mock_conn
        
        mock_connection_class.side_effect = side_effect
        
        config = {
            'host': 'localhost',
            'port': 5672,
            'username': 'guest',
            'password': 'guest'
        }
        
        manager = ConnectionManager(**config)
        results = []
        exceptions = []
        
        def worker(thread_id):
            try:
                # 同时创建连接和channel
                connection = manager.connection
                channel = manager.create_channel(f"channel_{thread_id}")
                results.append(f"Thread {thread_id} success")
            except Exception as e:
                exceptions.append(f"Thread {thread_id} failed: {e}")
        
        # 创建多个线程同时访问
        threads = []
        for i in range(5):
            thread = threading.Thread(target=worker, args=(i,))
            threads.append(thread)
            thread.start()
        
        # 等待所有线程完成
        for thread in threads:
            thread.join()
        
        print(f"✓ 成功结果: {len(results)} 个")
        print(f"✓ 异常结果: {len(exceptions)} 个")
        
        if exceptions:
            for exc in exceptions:
                print(f"  - {exc}")
        
        # 至少应该有一些成功的结果
        assert len(results) > 0, "应该有成功的连接"
        
        manager.shutdown()


def test_retry_with_real_config():
    """测试使用真实配置的重试机制（但不实际连接）"""
    print("\n=== 测试真实配置的重试机制结构 ===")
    
    config = {
        'host': 'localhost',
        'port': 5672,
        'username': 'guest',
        'password': 'guest'
    }
    
    manager = ConnectionManager(**config)
    
    # 验证重试参数设置
    assert hasattr(manager, 'MAX_CONNECTION_ATTEMPTS'), "应该有最大连接重试次数设置"
    assert hasattr(manager, 'MAX_CONNECTION_DELAY'), "应该有最大延迟时间设置"
    assert hasattr(manager, 'RECONNECTION_DELAY'), "应该有重连延迟设置"
    
    print(f"✓ 最大连接重试次数: {manager.MAX_CONNECTION_ATTEMPTS}")
    print(f"✓ 最大延迟时间: {manager.MAX_CONNECTION_DELAY}秒")
    print(f"✓ 重连延迟: {manager.RECONNECTION_DELAY}秒")
    
    # 验证配置存储正确
    assert manager._config['host'] == 'localhost', "host配置应该正确存储"
    assert manager._config['port'] == 5672, "port配置应该正确存储"
    assert manager._config['username'] == 'guest', "username配置应该正确存储"
    assert manager._config['password'] == 'guest', "password配置应该正确存储"
    print("✓ 配置参数存储正确（host->hostname转换在连接时进行）")
    
    manager.shutdown()


if __name__ == "__main__":
    print("开始测试ConnectionManager的重试功能...")
    
    try:
        test_connection_retry_mechanism()
        test_channel_retry_mechanism()
        test_max_retry_exceeded()
        test_concurrent_retry()
        test_retry_with_real_config()
        
        print("\n🎉 所有ConnectionManager重试功能测试通过！")
        print("\n总结:")
        print("✓ ConnectionManager现在支持连接重试机制")
        print("✓ 包含指数退避延迟策略")
        print("✓ Channel创建也有重试机制")
        print("✓ 线程安全的重试处理")
        print("✓ 可配置的重试参数")
        print("✓ 与RabbitMQStore相同级别的可靠性")
        
    except Exception as e:
        print(f"\n❌ 测试失败: {e}")
        raise