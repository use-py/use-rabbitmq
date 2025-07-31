import os
import threading
import time
from unittest.mock import Mock, patch

import pytest
from amqpstorm.exception import AMQPConnectionError

from use_rabbitmq import RabbitMQStore, useRabbitMQ, RabbitListener, useRabbitListener

# 设置测试环境变量
os.environ.setdefault("RABBITMQ_PASSWORD", "admin")


class TestRabbitMQStore:
    """RabbitMQStore类的单元测试"""

    def test_init_with_parameters(self):
        """测试初始化参数"""
        store = RabbitMQStore(
            host="test-host",
            port=5673,
            username="test-user",
            password="test-pass",
            confirm_delivery=False,
            client_name="test-client",
            use_connection_pool=False,
            use_channel_manager=False
        )
        
        assert store.parameters["hostname"] == "test-host"
        assert store.parameters["port"] == 5673
        assert store.parameters["username"] == "test-user"
        assert store.parameters["password"] == "test-pass"
        assert store.confirm_delivery is False
        assert store.client_name == "test-client"
        assert store.use_connection_pool is False
        assert store.use_channel_manager is False

    def test_init_with_env_variables(self):
        """测试使用环境变量初始化"""
        with patch.dict(os.environ, {
            'RABBITMQ_HOST': 'env-host',
            'RABBITMQ_PORT': '5674',
            'RABBITMQ_USERNAME': 'env-user',
            'RABBITMQ_PASSWORD': 'env-pass'
        }):
            store = RabbitMQStore(use_connection_pool=False, use_channel_manager=False)
            
            assert store.parameters["hostname"] == "env-host"
            assert store.parameters["port"] == 5674
            assert store.parameters["username"] == "env-user"
            assert store.parameters["password"] == "env-pass"

    def test_init_defaults(self):
        """测试默认参数"""
        # 清除环境变量以测试真正的默认值
        with patch.dict(os.environ, {}, clear=True):
            store = RabbitMQStore(use_connection_pool=False, use_channel_manager=False)
            
            assert store.parameters["hostname"] == "localhost"
            assert store.parameters["port"] == 5672
            assert store.parameters["username"] == "guest"
            assert store.parameters["password"] == "guest"
            assert store.confirm_delivery is True
            assert store.use_connection_pool is False
            assert store.use_channel_manager is False

    @patch('use_rabbitmq.amqpstorm.Connection')
    def test_create_connection_success(self, mock_connection):
        """测试成功创建连接"""
        mock_conn = Mock()
        mock_connection.return_value = mock_conn
        
        store = RabbitMQStore(use_connection_pool=False, use_channel_manager=False)
        connection = store._create_connection()
        
        assert connection == mock_conn
        # 检查是否调用了连接创建，但参数可能包含client_properties
        mock_connection.assert_called_once()

    @patch('use_rabbitmq.amqpstorm.Connection')
    @patch('use_rabbitmq.time.sleep')
    def test_create_connection_retry(self, mock_sleep, mock_connection):
        """测试连接重试机制"""
        mock_connection.side_effect = [
            AMQPConnectionError("Connection failed"),
            AMQPConnectionError("Connection failed"),
            Mock()  # 第三次成功
        ]
        
        store = RabbitMQStore(use_connection_pool=False, use_channel_manager=False)
        connection = store._create_connection()
        
        assert mock_connection.call_count == 3
        assert mock_sleep.call_count == 2
        assert connection is not None

    @patch('use_rabbitmq.amqpstorm.Connection')
    def test_create_connection_max_attempts(self, mock_connection):
        """测试连接最大重试次数"""
        mock_connection.side_effect = AMQPConnectionError("Connection failed")
        
        store = RabbitMQStore(use_connection_pool=False, use_channel_manager=False)
        store.MAX_CONNECTION_ATTEMPTS = 2
        
        with pytest.raises(AMQPConnectionError):
            store._create_connection()

    @patch('use_rabbitmq.amqpstorm.Connection')
    def test_connection_property(self, mock_connection):
        """测试connection属性"""
        mock_conn = Mock()
        mock_conn.is_open = True
        mock_connection.return_value = mock_conn
        
        store = RabbitMQStore(use_connection_pool=False, use_channel_manager=False)
        
        # 第一次访问创建连接
        connection1 = store.connection
        assert connection1 == mock_conn
        
        # 第二次访问返回相同连接
        connection2 = store.connection
        assert connection2 == mock_conn
        assert mock_connection.call_count == 1

    @patch('use_rabbitmq.amqpstorm.Connection')
    def test_connection_property_reconnect(self, mock_connection):
        """测试连接断开后重连"""
        mock_conn1 = Mock()
        mock_conn1.is_open = False
        mock_conn2 = Mock()
        mock_conn2.is_open = True
        mock_connection.return_value = mock_conn2
        
        store = RabbitMQStore(use_connection_pool=False, use_channel_manager=False)
        store._connection = mock_conn1
        
        connection = store.connection
        assert connection == mock_conn2
        assert mock_connection.call_count == 1

    def test_connection_deleter(self):
        """测试连接删除器"""
        store = RabbitMQStore(use_connection_pool=False, use_channel_manager=False)
        mock_conn = Mock()
        mock_conn.is_open = True
        store._connection = mock_conn
        
        del store.connection
        
        # 在非连接池模式下，连接应该被关闭
        mock_conn.close.assert_called_once()
        assert store._connection is None

    @patch('use_rabbitmq.amqpstorm.Connection')
    def test_channel_property(self, mock_connection):
        """测试channel属性"""
        mock_conn = Mock()
        mock_conn.is_open = True
        mock_channel = Mock()
        mock_channel.is_open = True
        mock_conn.channel.return_value = mock_channel
        mock_connection.return_value = mock_conn
        
        store = RabbitMQStore(use_connection_pool=False, use_channel_manager=False)
        
        channel = store.channel
        assert channel == mock_channel
        mock_channel.confirm_deliveries.assert_called_once()

    @patch('use_rabbitmq.amqpstorm.Connection')
    def test_channel_property_no_confirm(self, mock_connection):
        """测试不确认交付的channel"""
        mock_conn = Mock()
        mock_conn.is_open = True
        mock_channel = Mock()
        mock_channel.is_open = True
        mock_conn.channel.return_value = mock_channel
        mock_connection.return_value = mock_conn
        
        store = RabbitMQStore(confirm_delivery=False, use_connection_pool=False, use_channel_manager=False)
        
        channel = store.channel
        assert channel == mock_channel
        mock_channel.confirm_deliveries.assert_not_called()

    def test_channel_deleter(self):
        """测试channel删除器"""
        store = RabbitMQStore(use_connection_pool=False, use_channel_manager=False)
        mock_channel = Mock()
        mock_channel.is_open = True
        store._channel = mock_channel
        
        del store.channel
        
        # 在非通道管理器模式下，通道应该被关闭
        mock_channel.close.assert_called_once()
        assert store._channel is None

    @patch('use_rabbitmq.amqpstorm.Connection')
    def test_declare_queue_success(self, mock_connection):
        """测试声明队列成功"""
        mock_conn = Mock()
        mock_conn.is_open = True
        mock_channel = Mock()
        mock_channel.is_open = True
        mock_conn.channel.return_value = mock_channel
        mock_connection.return_value = mock_conn
        
        store = RabbitMQStore(use_connection_pool=False, use_channel_manager=False)
        store.declare_queue("test-queue")
        
        mock_channel.queue.declare.assert_called_with("test-queue", passive=True, durable=True)

    @patch('use_rabbitmq.amqpstorm.Connection')
    def test_declare_queue_with_kwargs(self, mock_connection):
        """测试声明队列时传递额外参数"""
        mock_conn = Mock()
        mock_conn.is_open = True
        mock_channel = Mock()
        mock_channel.is_open = True
        mock_conn.channel.return_value = mock_channel
        mock_connection.return_value = mock_conn
        
        store = RabbitMQStore(use_connection_pool=False, use_channel_manager=False)
        store.declare_queue("test-queue", durable=False, auto_delete=True)
        
        mock_channel.queue.declare.assert_called_with(
            "test-queue", passive=True, durable=False
        )

    @patch('use_rabbitmq.amqpstorm.Connection')
    def test_send_message_success(self, mock_connection):
        """测试发送消息成功"""
        mock_conn = Mock()
        mock_conn.is_open = True
        mock_channel = Mock()
        mock_channel.is_open = True
        mock_conn.channel.return_value = mock_channel
        mock_connection.return_value = mock_conn
        
        store = RabbitMQStore(use_connection_pool=False, use_channel_manager=False)
        result = store.send("test-queue", "test message")
        
        assert result == "test message"
        mock_channel.basic.publish.assert_called_once_with(
            "test message", "test-queue", properties=None
        )

    @patch('use_rabbitmq.amqpstorm.Connection')
    @patch('use_rabbitmq.time.sleep')
    def test_send_message_retry(self, mock_sleep, mock_connection):
        """测试发送消息重试"""
        mock_conn = Mock()
        mock_conn.is_open = True
        mock_channel = Mock()
        mock_channel.is_open = True
        mock_conn.channel.return_value = mock_channel
        mock_connection.return_value = mock_conn
        
        # 第一次失败，第二次成功
        mock_channel.basic.publish.side_effect = [Exception("Send failed"), None]
        
        store = RabbitMQStore(use_connection_pool=False, use_channel_manager=False)
        result = store.send("test-queue", "test message")
        
        assert result == "test message"
        assert mock_channel.basic.publish.call_count == 2
        mock_sleep.assert_called_once()

    @patch('use_rabbitmq.amqpstorm.Connection')
    def test_flush_queue(self, mock_connection):
        """测试清空队列"""
        mock_conn = Mock()
        mock_conn.is_open = True
        mock_channel = Mock()
        mock_channel.is_open = True
        mock_conn.channel.return_value = mock_channel
        mock_connection.return_value = mock_conn
        
        store = RabbitMQStore(use_connection_pool=False, use_channel_manager=False)
        store.flush_queue("test-queue")
        
        mock_channel.queue.purge.assert_called_once_with("test-queue")

    @patch('use_rabbitmq.amqpstorm.Connection')
    def test_get_message_counts(self, mock_connection):
        """测试获取消息数量"""
        mock_conn = Mock()
        mock_conn.is_open = True
        mock_channel = Mock()
        mock_channel.is_open = True
        mock_conn.channel.return_value = mock_channel
        mock_connection.return_value = mock_conn
        
        mock_channel.queue.declare.return_value = {"message_count": 5}
        
        store = RabbitMQStore(use_connection_pool=False, use_channel_manager=False)
        count = store.get_message_counts("test-queue")
        
        assert count == 5
        mock_channel.queue.declare.assert_called_once_with(
            "test-queue", passive=True, durable=False
        )

    def test_shutdown(self):
        """测试关闭"""
        store = RabbitMQStore(use_connection_pool=False, use_channel_manager=False)
        mock_conn = Mock()
        store._connection = mock_conn
        
        store.shutdown()
        
        # 检查连接是否被关闭
        mock_conn.close.assert_called_once()

    @patch('use_rabbitmq.amqpstorm.Connection')
    def test_listener_decorator(self, mock_connection):
        """测试监听器装饰器"""
        mock_conn = Mock()
        mock_conn.is_open = True
        mock_channel = Mock()
        mock_channel.is_open = True
        mock_conn.channel.return_value = mock_channel
        mock_connection.return_value = mock_conn
        
        store = RabbitMQStore(use_connection_pool=False, use_channel_manager=False)
        
        @store.listener("test-queue")
        def callback(message):
            pass
        
        assert isinstance(callback, threading.Thread)
        mock_channel.queue.declare.assert_called_once_with("test-queue", passive=True, durable=True)

    @patch('use_rabbitmq.amqpstorm.Connection')
    def test_stop_listener(self, mock_connection):
        """测试停止监听器"""
        mock_conn = Mock()
        mock_conn.is_open = True
        mock_channel = Mock()
        mock_channel.is_open = True
        mock_conn.channel.return_value = mock_channel
        mock_connection.return_value = mock_conn
        
        store = RabbitMQStore(use_connection_pool=False, use_channel_manager=False)
        store._channel = mock_channel
        
        store.stop_listener("test-queue")
        
        mock_channel.basic.cancel.assert_called_once_with("test-queue")


class TestRabbitListener:
    """RabbitListener类的单元测试"""

    def test_init(self):
        """测试RabbitListener初始化"""
        mock_store = Mock()
        listener = RabbitListener(
            mock_store,
            queue_name="test-queue",
            no_ack=True,
            prefetch=5
        )
        
        assert listener.instance == mock_store
        assert listener.queue_name == "test-queue"
        assert listener.no_ack is True
        assert listener.kwargs == {"prefetch": 5}

    def test_call(self):
        """测试RabbitListener调用"""
        mock_store = Mock()
        mock_listener_func = Mock()
        mock_store.listener.return_value = mock_listener_func
        
        listener = RabbitListener(mock_store, queue_name="test-queue")
        
        def callback(message):
            pass
        
        result = listener(callback)
        
        mock_store.listener.assert_called_once_with("test-queue", False)
        mock_listener_func.assert_called_once_with(callback)
        assert result == mock_listener_func.return_value


class TestAliases:
    """测试别名"""

    def test_use_rabbitmq_alias(self):
        """测试useRabbitMQ别名"""
        assert useRabbitMQ is RabbitMQStore

    def test_use_rabbit_listener_alias(self):
        """测试useRabbitListener别名"""
        assert useRabbitListener is RabbitListener


# 集成测试（需要真实的RabbitMQ服务器）
@pytest.fixture
def rabbitmq():
    """RabbitMQ实例fixture"""
    store = useRabbitMQ(
        host="localhost", 
        port=5672, 
        username="admin",
        use_connection_pool=False,
        use_channel_manager=False
    )
    yield store
    # 确保测试结束后资源被清理
    store.shutdown()


@pytest.mark.integration
def test_rabbitmq_connection(rabbitmq):
    """集成测试：连接"""
    assert rabbitmq.connection.is_open is True


@pytest.mark.integration
def test_rabbitmq_channel(rabbitmq):
    """集成测试：通道"""
    assert rabbitmq.channel.is_open is True


@pytest.mark.integration
def test_send_and_receive(rabbitmq):
    """集成测试：发送和接收消息"""
    queue_name = "test-integration-queue"
    rabbitmq.declare_queue(queue_name)
    rabbitmq.flush_queue(queue_name)
    
    # 发送消息
    message = "integration test message"
    result = rabbitmq.send(queue_name=queue_name, message=message)
    assert result == message
    
    # 检查消息数量
    count = rabbitmq.get_message_counts(queue_name)
    assert count == 1
    
    # 获取消息
    received_message = rabbitmq.channel.basic.get(queue_name)
    assert received_message.body == message


@pytest.mark.integration
def test_flush_queue_integration(rabbitmq):
    """集成测试：清空队列"""
    queue_name = "test-flush-queue"
    rabbitmq.declare_queue(queue_name)
    
    # 发送消息
    rabbitmq.send(queue_name=queue_name, message="test message")
    assert rabbitmq.get_message_counts(queue_name) == 1
    
    # 清空队列
    rabbitmq.flush_queue(queue_name)
    assert rabbitmq.get_message_counts(queue_name) == 0


@pytest.mark.integration
def test_listener_integration(rabbitmq):
    """集成测试：监听器"""
    queue_name = "test-listener-queue"
    rabbitmq.declare_queue(queue_name)
    rabbitmq.flush_queue(queue_name)
    
    # 发送测试消息
    test_message = "listener test message"
    rabbitmq.send(queue_name=queue_name, message=test_message)
    
    received_messages = []
    
    @rabbitmq.listener(queue_name)
    def callback(message):
        received_messages.append(message.body)
        message.ack()
        # 停止监听并关闭连接
        rabbitmq.stop_listener(queue_name)
    
    # 保存线程引用以便后续清理
    listener_thread = callback
    
    # 等待消息处理
    start_time = time.time()
    timeout = 5  # 5秒超时
    while len(received_messages) == 0 and time.time() - start_time < timeout:
        time.sleep(0.1)
    
    # 确保资源清理
    rabbitmq.stop_listener(queue_name)
    rabbitmq.shutdown()
    
    # 确保线程结束
    if listener_thread and listener_thread.is_alive():
        listener_thread.join(timeout=1)
    
    assert len(received_messages) == 1
    assert received_messages[0] == test_message
