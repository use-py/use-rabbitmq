#!/usr/bin/env python3
"""
增强功能测试

测试连接池管理、客户端自定义名称支持和单连接多通道支持功能。
"""

import pytest
import threading
import time
from unittest.mock import patch, MagicMock
from use_rabbitmq import RabbitMQStore, ConnectionPool, ChannelManager


class TestConnectionPool:
    """连接池测试"""
    
    def test_singleton_pattern(self):
        """测试连接池单例模式"""
        pool1 = ConnectionPool()
        pool2 = ConnectionPool()
        assert pool1 is pool2
    
    def test_pool_key_generation(self):
        """测试连接池键生成"""
        pool = ConnectionPool()
        params1 = {'hostname': 'localhost', 'port': 5672, 'username': 'guest'}
        params2 = {'hostname': 'localhost', 'port': 5672, 'username': 'guest'}
        params3 = {'hostname': 'localhost', 'port': 5673, 'username': 'guest'}
        
        key1 = pool._get_pool_key(params1)
        key2 = pool._get_pool_key(params2)
        key3 = pool._get_pool_key(params3)
        
        assert key1 == key2
        assert key1 != key3
    
    @patch('amqpstorm.Connection')
    def test_connection_creation_with_client_name(self, mock_connection):
        """测试带客户端名称的连接创建"""
        mock_conn = MagicMock()
        mock_conn.is_open = True
        mock_connection.return_value = mock_conn
        
        pool = ConnectionPool()
        params = {'hostname': 'localhost', 'port': 5672, 'username': 'guest'}
        client_name = 'test-client'
        
        connection = pool.get_connection(params, client_name)
        
        # 验证连接创建时包含客户端属性
        mock_connection.assert_called_once()
        call_args = mock_connection.call_args[1]
        assert 'client_properties' in call_args
        assert call_args['client_properties']['client_name'] == client_name
        assert connection is mock_conn


class TestChannelManager:
    """通道管理器测试"""
    
    def test_channel_manager_initialization(self):
        """测试通道管理器初始化"""
        mock_connection = MagicMock()
        mock_connection.is_open = True
        
        manager = ChannelManager(mock_connection, max_channels=5)
        assert manager.connection is mock_connection
        assert manager.max_channels == 5
        assert manager._channel_count == 0
    
    def test_channel_acquisition_and_release(self):
        """测试通道获取和释放"""
        mock_connection = MagicMock()
        mock_connection.is_open = True
        
        mock_channel = MagicMock()
        mock_channel.is_open = True
        mock_connection.channel.return_value = mock_channel
        
        manager = ChannelManager(mock_connection, max_channels=2)
        
        # 获取通道
        channel = manager._acquire_channel()
        assert channel is mock_channel
        assert manager._channel_count == 1
        
        # 释放通道
        manager._release_channel(channel)
        assert manager._channels.qsize() == 1
    
    def test_context_manager(self):
        """测试通道上下文管理器"""
        mock_connection = MagicMock()
        mock_connection.is_open = True
        
        mock_channel = MagicMock()
        mock_channel.is_open = True
        mock_connection.channel.return_value = mock_channel
        
        manager = ChannelManager(mock_connection)
        
        with manager.get_channel() as channel:
            assert channel is mock_channel
        
        # 通道应该被返回到池中
        assert manager._channels.qsize() == 1


class TestRabbitMQStoreEnhancements:
    """RabbitMQStore增强功能测试"""
    
    def test_initialization_with_enhancements(self):
        """测试带增强功能的初始化"""
        mq = RabbitMQStore(
            host="localhost",
            client_name="test-client",
            use_connection_pool=True,
            use_channel_manager=True,
            max_channels=10
        )
        
        assert mq.client_name == "test-client"
        assert mq.use_connection_pool is True
        assert mq.use_channel_manager is True
        assert mq.max_channels == 10
        assert mq._connection_pool is not None
    
    def test_auto_generated_client_name(self):
        """测试自动生成的客户端名称"""
        mq = RabbitMQStore(host="localhost")
        assert mq.client_name.startswith("use-rabbitmq-")
        assert len(mq.client_name) > len("use-rabbitmq-")
    
    def test_client_info(self):
        """测试客户端信息获取"""
        mq = RabbitMQStore(
            host="localhost",
            port=5672,
            username="guest",
            password="guest",
            client_name="info-test-client",
            use_connection_pool=True,
            use_channel_manager=True
        )
        
        info = mq.get_client_info()
        
        assert info['client_name'] == "info-test-client"
        assert info['use_connection_pool'] is True
        assert info['use_channel_manager'] is True
        assert 'connection_parameters' in info
        assert info['connection_parameters']['hostname'] == "localhost"
        assert info['connection_parameters']['port'] == 5672
    
    @patch('amqpstorm.Connection')
    def test_connection_with_client_properties(self, mock_connection):
        """测试连接包含客户端属性"""
        mock_conn = MagicMock()
        mock_conn.is_open = True
        mock_connection.return_value = mock_conn
        
        mq = RabbitMQStore(
            host="localhost",
            client_name="property-test-client",
            use_connection_pool=False  # 禁用连接池以测试直接连接
        )
        
        # 触发连接创建
        _ = mq.connection
        
        # 验证连接创建时包含客户端属性
        mock_connection.assert_called_once()
        call_args = mock_connection.call_args[1]
        assert 'client_properties' in call_args
        assert call_args['client_properties']['client_name'] == "property-test-client"
    
    def test_stats_methods(self):
        """测试统计信息方法"""
        mq = RabbitMQStore(
            host="localhost",
            use_connection_pool=True,
            use_channel_manager=True
        )
        
        # 在没有活动连接时，统计信息应该为None或空
        pool_stats = mq.get_connection_pool_stats()
        channel_stats = mq.get_channel_manager_stats()
        
        # 连接池统计应该存在但可能为空
        assert pool_stats is not None
        # 通道管理器统计在没有连接时应该为None
        assert channel_stats is None
    
    def test_backward_compatibility(self):
        """测试向后兼容性"""
        # 使用原有的参数创建实例
        mq = RabbitMQStore(
            host="localhost",
            port=5672,
            username="guest",
            password="guest",
            confirm_delivery=True
        )
        
        # 验证原有功能仍然可用
        assert mq.confirm_delivery is True
        assert mq.parameters['hostname'] == "localhost"
        assert mq.parameters['port'] == 5672
        assert mq.parameters['username'] == "guest"
        assert mq.parameters['password'] == "guest"
        
        # 验证新功能有默认值
        assert mq.use_connection_pool is True
        assert mq.use_channel_manager is True
        assert mq.max_channels == 20


class TestConcurrency:
    """并发测试"""
    
    def test_thread_safety(self):
        """测试线程安全性"""
        results = []
        errors = []
        
        def create_client(thread_id):
            try:
                mq = RabbitMQStore(
                    host="localhost",
                    client_name=f"thread-{thread_id}",
                    use_connection_pool=True,
                    use_channel_manager=True
                )
                results.append(mq.client_name)
            except Exception as e:
                errors.append(e)
        
        # 创建多个线程
        threads = []
        for i in range(5):
            thread = threading.Thread(target=create_client, args=(i,))
            threads.append(thread)
            thread.start()
        
        # 等待所有线程完成
        for thread in threads:
            thread.join()
        
        # 验证结果
        assert len(errors) == 0, f"线程执行出错: {errors}"
        assert len(results) == 5
        assert len(set(results)) == 5  # 所有客户端名称应该是唯一的


if __name__ == "__main__":
    # 运行基本测试
    print("运行增强功能测试...")
    
    # 测试连接池
    print("测试连接池...")
    test_pool = TestConnectionPool()
    test_pool.test_singleton_pattern()
    test_pool.test_pool_key_generation()
    print("连接池测试通过")
    
    # 测试通道管理器
    print("测试通道管理器...")
    test_channel = TestChannelManager()
    test_channel.test_channel_manager_initialization()
    print("通道管理器测试通过")
    
    # 测试RabbitMQStore增强功能
    print("测试RabbitMQStore增强功能...")
    test_store = TestRabbitMQStoreEnhancements()
    test_store.test_initialization_with_enhancements()
    test_store.test_auto_generated_client_name()
    test_store.test_client_info()
    test_store.test_backward_compatibility()
    print("RabbitMQStore增强功能测试通过")
    
    # 测试并发
    print("测试并发安全性...")
    test_concurrency = TestConcurrency()
    test_concurrency.test_thread_safety()
    print("并发测试通过")
    
    print("\n所有测试通过！")
