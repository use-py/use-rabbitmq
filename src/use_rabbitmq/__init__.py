import logging
import os
import threading
import time
from typing import Callable, Optional, Union, Any, Dict

import amqpstorm
from amqpstorm import Message
from amqpstorm.exception import AMQPConnectionError, AMQPChannelError

logger = logging.getLogger(__name__)


class RabbitMQStore:
    """
    RabbitMQ消息队列存储和消费类。

    该类提供了与RabbitMQ交互的各种方法,包括连接、声明队列、发送消息、获取消息数量和消费消息等。
    它还包含了重试机制和异常处理,以确保连接的可靠性和消息的正确传递。
    
    支持上下文管理器模式:
        with RabbitMQStore() as mq:
            mq.send('queue', 'message')
        # 自动调用shutdown()
    """

    MAX_SEND_ATTEMPTS: int = 6  # 最大发送重试次数
    MAX_CONNECTION_ATTEMPTS: float = float("inf")  # 最大连接重试次数
    MAX_CONNECTION_DELAY: int = 2 ** 5  # 最大延迟时间
    RECONNECTION_DELAY: int = 1

    def __init__(
            self,
            *,
            confirm_delivery: bool = True,
            host: Optional[str] = None,
            port: Optional[int] = None,
            username: Optional[str] = None,
            password: Optional[str] = None,
            **kwargs,
    ):
        """
        :param confirm_delivery: 是否开启消息确认
        :param host: RabbitMQ host
        :param port: RabbitMQ port
        :param username: RabbitMQ username
        :param password: RabbitMQ password
        :param kwargs: RabbitMQ parameters
        """
        self.__shutdown = False
        self._lock = threading.Lock()
        self.parameters: Dict[str, Any] = {
            "hostname": host or os.environ.get("RABBITMQ_HOST", "localhost"),
            "port": port or int(os.environ.get("RABBITMQ_PORT", 5672)),
            "username": username or os.environ.get("RABBITMQ_USERNAME", "guest"),
            "password": password or os.environ.get("RABBITMQ_PASSWORD", "guest"),
        }
        if kwargs:
            self.parameters.update(kwargs)
        self.confirm_delivery = confirm_delivery
        self._connection: Optional[amqpstorm.Connection] = None
        self._channel: Optional[amqpstorm.Channel] = None

    def _create_connection(self) -> amqpstorm.Connection:
        attempts = 1
        reconnection_delay = self.RECONNECTION_DELAY
        while attempts <= self.MAX_CONNECTION_ATTEMPTS:
            try:
                connector = amqpstorm.Connection(**self.parameters)
                if attempts > 1:
                    logger.warning(
                        f"RabbitmqStore connection succeeded after {attempts} attempts",
                    )
                return connector
            except AMQPConnectionError as exc:
                logger.warning(
                    f"RabbitmqStore connection error<{exc}>; retrying in {reconnection_delay} seconds"
                )
                attempts += 1
                time.sleep(reconnection_delay)
                reconnection_delay = min(
                    reconnection_delay * 2, self.MAX_CONNECTION_DELAY
                )
        raise AMQPConnectionError(
            "RabbitmqStore connection error, max attempts reached"
        )

    @property
    def connection(self) -> amqpstorm.Connection:
        with self._lock:
            if self._connection is None or not self._connection.is_open:
                self._connection = self._create_connection()
            return self._connection

    @connection.deleter
    def connection(self) -> None:
        with self._lock:
            # 先删除channel
            if self._channel:
                try:
                    if self._channel.is_open:
                        self._channel.close()
                except Exception as exc:
                    logger.exception(f"RabbitmqStore channel close error<{exc}>")
                finally:
                    self._channel = None
            
            # 再删除connection
            if self._connection:
                try:
                    if self._connection.is_open:
                        self._connection.close()
                except Exception as exc:
                    logger.exception(f"RabbitmqStore connection close error<{exc}>")
                finally:
                    self._connection = None

    @property
    def channel(self) -> amqpstorm.Channel:
        with self._lock:
            # 检查现有连接和通道是否可用
            if (self._connection and self._connection.is_open and 
                self._channel and self._channel.is_open):
                return self._channel
            
            # 确保有有效的连接
            if self._connection is None or not self._connection.is_open:
                self._connection = self._create_connection()
            
            # 创建新的通道
            self._channel = self._connection.channel()
            if self.confirm_delivery and self._channel:
                self._channel.confirm_deliveries()
            
            # 确保返回有效的通道
            if self._channel is None:
                raise RuntimeError("Failed to create channel")
            return self._channel

    @channel.deleter
    def channel(self):
        with self._lock:
            if self._channel:
                try:
                    if self._channel.is_open:
                        self._channel.close()
                except Exception as exc:
                    logger.exception(f"RabbitmqStore channel close error<{exc}>")
                finally:
                    self._channel = None

    def shutdown(self):
        """安全关闭RabbitMQ连接和通道"""
        self.__shutdown = True
        with self._lock:
            try:
                # 先停止消费
                if self._channel and self._channel.is_open:
                    try:
                        self._channel.stop_consuming()
                    except Exception as exc:
                        logger.warning(f"Error stopping consuming during shutdown: {exc}")
                
                # 关闭通道
                if self._channel:
                    try:
                        if self._channel.is_open:
                            self._channel.close()
                    except Exception as exc:
                        logger.warning(f"Error closing channel during shutdown: {exc}")
                    finally:
                        self._channel = None
                
                # 关闭连接
                if self._connection:
                    try:
                        if self._connection.is_open:
                            self._connection.close()
                    except Exception as exc:
                        logger.warning(f"Error closing connection during shutdown: {exc}")
                    finally:
                        self._connection = None
            except Exception as exc:
                logger.error(f"Error during shutdown: {exc}")

    def declare_queue(self, queue_name: str, durable: bool = True, **kwargs):
        """声明队列"""
        try:
            self.channel.queue.declare(queue_name, passive=True, durable=durable)
        except amqpstorm.AMQPChannelError as exc:
            if exc.error_code != 404:
                raise exc
            return self.channel.queue.declare(queue_name, durable=durable, **kwargs)

    def send(
            self,
            queue_name: str,
            message: Union[str, bytes],
            properties: Optional[dict] = None,
            **kwargs,
    ) -> Union[str, bytes]:
        """发送消息"""
        attempts = 1
        while True:
            try:
                self.channel.basic.publish(
                    message, queue_name, properties=properties, **kwargs
                )
                return message
            except Exception as exc:
                del self.connection
                attempts += 1
                if attempts > self.MAX_SEND_ATTEMPTS:
                    raise exc
                time.sleep(min(attempts * 0.5, 2))  # 添加重试延迟

    def flush_queue(self, queue_name: str):
        """清空队列"""
        self.channel.queue.purge(queue_name)

    def get_message_counts(self, queue_name: str) -> int:
        """获取消息数量"""
        queue_response = self.channel.queue.declare(
            queue_name, passive=True, durable=False
        )
        return queue_response.get("message_count", 0)

    def start_consuming(
            self, queue_name: str, callback: Callable, prefetch=1, **kwargs
    ):
        """开始消费"""
        self.__shutdown = False
        no_ack = kwargs.pop("no_ack", False)
        reconnection_delay = self.RECONNECTION_DELAY

        while not self.__shutdown:
            try:
                self.channel.basic.qos(prefetch_count=prefetch)
                self.channel.basic.consume(
                    queue=queue_name, callback=callback, no_ack=no_ack, **kwargs
                )
                self.channel.start_consuming(to_tuple=False)
            except AMQPChannelError as exc:
                if self.__shutdown:
                    break
                logger.error(f"RabbitmqStore channel error: {exc}")
                del self.channel
                time.sleep(reconnection_delay)
                reconnection_delay = min(
                    reconnection_delay * 2, self.MAX_CONNECTION_DELAY
                )

            except AMQPConnectionError as exc:
                if self.__shutdown:
                    break
                logger.error(
                    f"RabbitmqStore consume connection error<{exc}> reconnecting..."
                )
                del self.connection
                time.sleep(reconnection_delay)
                reconnection_delay = min(
                    reconnection_delay * 2, self.MAX_CONNECTION_DELAY
                )
            except Exception as e:
                if self.__shutdown:
                    break
                logger.exception(f"RabbitmqStore consume error<{e}>, reconnecting...")
                del self.connection
                time.sleep(reconnection_delay)
                reconnection_delay = min(
                    reconnection_delay * 2, self.MAX_CONNECTION_DELAY
                )
            finally:
                # 确保在退出时停止消费
                try:
                    if self._channel and self._channel.is_open:
                        self._channel.stop_consuming()
                except Exception as exc:
                    logger.warning(f"Error stopping consuming: {exc}")
                if self.__shutdown:
                    break

    def __enter__(self):
        """上下文管理器入口"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器退出，自动清理资源"""
        self.shutdown()
        return False  # 不抑制异常
    
    def __del__(self):
        try:
            self.shutdown()
        except Exception:
            # 忽略析构函数中的异常，避免影响程序退出
            pass

    def listener(self, queue_name: str, no_ack: bool = False, **kwargs):
        self.declare_queue(queue_name)

        def wrapper(callback: Callable[[Message], Any]):
            logger.info(f"RabbitMQStore consume {queue_name}")

            def target():
                self.start_consuming(queue_name, callback, no_ack=no_ack, **kwargs)

            thread = threading.Thread(target=target, daemon=True)  # 使用daemon线程，确保主程序退出时线程也会退出
            thread.start()
            return thread

        return wrapper

    def stop_listener(self, queue_name: str) -> None:
        """停止监听指定队列"""
        try:
            if self._channel and self._channel.is_open:
                # 停止消费
                self._channel.stop_consuming()
                # 取消消费者
                self._channel.basic.cancel(queue_name)
        except Exception as exc:
            logger.warning(f"Error canceling consumer for queue {queue_name}: {exc}")
        finally:
            # 设置关闭标志
            self.__shutdown = True
            # 关闭连接
            try:
                del self.channel
                del self.connection
            except Exception as exc:
                logger.warning(f"Error closing connection: {exc}")


class RabbitListener:
    def __init__(self, instance: RabbitMQStore, *, queue_name: str, no_ack: bool = False, **kwargs):
        self.instance = instance
        self.queue_name = queue_name
        self.no_ack = no_ack
        self.kwargs = kwargs

    def __call__(self, callback: Callable[[amqpstorm.Message], None]):
        listener = self.instance.listener(self.queue_name, self.no_ack, **self.kwargs)
        return listener(callback)


class RabbitConnectionFactory:
    """
    RabbitMQ连接工厂类。
    
    该类实现了工厂模式，用于创建和管理RabbitMQ连接实例。
    支持单例模式、连接池管理、client_name设置和多channel管理等功能。
    
    特性:
    - 支持设置client_name（类似Java客户端）
    - 一个连接可以创建多个channel
    - 连接复用和管理
    - 线程安全
    
    示例:
        # 创建工厂实例
        factory = RabbitConnectionFactory(client_name="MyApp")
        
        # 获取连接管理器
        conn_manager = factory.get_connection_manager("default")
        
        # 创建多个channel
        channel1 = conn_manager.create_channel("publisher")
        channel2 = conn_manager.create_channel("consumer")
        
        # 关闭所有连接
        factory.shutdown_all()
    """
    
    def __init__(self, client_name: Optional[str] = None, default_config: Optional[Dict[str, Any]] = None):
        """
        初始化连接工厂。
        
        :param client_name: 客户端名称，会显示在RabbitMQ管理界面
        :param default_config: 默认连接配置
        """
        self._connection_managers: Dict[str, 'ConnectionManager'] = {}
        self._lock = threading.Lock()
        self._client_name = client_name or f"rabbitConnectionFactory#{id(self):x}"
        self._default_config = default_config or {
            "host": os.environ.get("RABBITMQ_HOST", "localhost"),
            "port": int(os.environ.get("RABBITMQ_PORT", 5672)),
            "username": os.environ.get("RABBITMQ_USERNAME", "guest"),
            "password": os.environ.get("RABBITMQ_PASSWORD", "guest"),
            "confirm_delivery": True
        }
    
    def get_connection_manager(self, name: str = "default", **config) -> 'ConnectionManager':
        """
        获取连接管理器（单例模式）。
        
        :param name: 连接名称
        :param config: 连接配置（仅在首次创建时使用）
        :return: ConnectionManager实例
        """
        with self._lock:
            if name not in self._connection_managers:
                # 合并配置
                merged_config = self._default_config.copy()
                merged_config.update(config)
                # 添加client_name
                merged_config["client_properties"] = {
                    "connection_name": f"{self._client_name}#{name}"
                }
                self._connection_managers[name] = ConnectionManager(**merged_config)
            return self._connection_managers[name]
    
    def create_connection(self, **config) -> RabbitMQStore:
        """
        创建新的RabbitMQ连接实例（兼容旧接口）。
        
        :param config: 连接配置
        :return: RabbitMQStore实例
        """
        merged_config = self._default_config.copy()
        merged_config.update(config)
        # 添加client_name
        if "client_properties" not in merged_config:
            merged_config["client_properties"] = {}
        merged_config["client_properties"]["connection_name"] = self._client_name
        
        return RabbitMQStore(**merged_config)
    
    def remove_connection_manager(self, name: str) -> bool:
        """
        移除并关闭指定名称的连接管理器。
        
        :param name: 连接名称
        :return: 是否成功移除
        """
        with self._lock:
            if name in self._connection_managers:
                manager = self._connection_managers.pop(name)
                try:
                    manager.shutdown()
                    return True
                except Exception as exc:
                    logger.warning(f"Error shutting down connection manager '{name}': {exc}")
                    return False
            return False
    
    def list_connections(self) -> list:
        """
        列出所有已创建的连接名称。
        
        :return: 连接名称列表
        """
        with self._lock:
            return list(self._connection_managers.keys())
    
    def shutdown_all(self):
        """
        关闭所有管理的连接。
        """
        with self._lock:
            for name, manager in self._connection_managers.items():
                try:
                    manager.shutdown()
                except Exception as exc:
                    logger.warning(f"Error shutting down connection manager '{name}': {exc}")
            self._connection_managers.clear()
    
    def __enter__(self):
        """上下文管理器入口"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器退出，自动清理所有连接"""
        self.shutdown_all()
        return False
    
    def listener(self, queue_name: str, channel_name: str = "consumer", no_ack: bool = False, connection_name: str = "default", **kwargs):
        """
        装饰器：在指定连接的指定channel上监听队列。
        
        :param queue_name: 队列名称
        :param channel_name: channel名称
        :param no_ack: 是否自动确认
        :param connection_name: 连接名称
        :param kwargs: 其他参数
        """
        def wrapper(callback: Callable[[amqpstorm.Message], Any]):
            logger.info(f"RabbitConnectionFactory listener on {connection_name}#{channel_name} for queue {queue_name}")
            
            def target():
                # 获取连接管理器和channel
                conn_manager = self.get_connection_manager(connection_name)
                channel = conn_manager.create_channel(channel_name, confirm_delivery=False)
                
                # 声明队列
                channel.queue.declare(queue_name, durable=True)
                
                # 开始消费
                try:
                    channel.basic.qos(prefetch_count=1)
                    channel.basic.consume(queue=queue_name, callback=callback, no_ack=no_ack, **kwargs)
                    channel.start_consuming(to_tuple=False)
                except Exception as exc:
                    logger.error(f"Error in listener for {queue_name}: {exc}")
                    raise
            
            thread = threading.Thread(target=target, daemon=True)
            thread.start()
            return thread
        
        return wrapper
    
    def __del__(self):
        """析构函数，确保资源清理"""
        try:
            self.shutdown_all()
        except Exception:
            # 忽略析构函数中的异常
            pass


class ConnectionManager:
    """
    单个RabbitMQ连接的管理器，支持多个Channel。
    
    该类负责管理一个RabbitMQ连接和多个命名的Channel，
    提供线程安全的Channel创建、获取、关闭等操作，
    并包含连接重试机制以确保可靠性。
    """
    
    MAX_CONNECTION_ATTEMPTS: float = float("inf")  # 最大连接重试次数
    MAX_CONNECTION_DELAY: int = 2 ** 5  # 最大延迟时间
    RECONNECTION_DELAY: int = 1
    
    def __init__(self, **config):
        """
        初始化连接管理器。
        
        :param config: 连接配置参数
        """
        self._config = config
        self._connection: Optional[amqpstorm.Connection] = None
        self._channels: Dict[str, amqpstorm.Channel] = {}
        self._lock = threading.Lock()
        self._shutdown = False
    
    def _create_connection(self) -> amqpstorm.Connection:
        """创建RabbitMQ连接，包含重试机制"""
        attempts = 1
        reconnection_delay = self.RECONNECTION_DELAY
        
        # 转换配置参数以适配amqpstorm.Connection
        connection_config = self._config.copy()
        if 'host' in connection_config:
            connection_config['hostname'] = connection_config.pop('host')
        
        while attempts <= self.MAX_CONNECTION_ATTEMPTS:
            try:
                connector = amqpstorm.Connection(**connection_config)
                if attempts > 1:
                    logger.warning(
                        f"ConnectionManager connection succeeded after {attempts} attempts",
                    )
                return connector
            except AMQPConnectionError as exc:
                logger.warning(
                    f"ConnectionManager connection error<{exc}>; retrying in {reconnection_delay} seconds"
                )
                attempts += 1
                time.sleep(reconnection_delay)
                reconnection_delay = min(
                    reconnection_delay * 2, self.MAX_CONNECTION_DELAY
                )
        
        raise AMQPConnectionError(
            "ConnectionManager connection error, max attempts reached"
        )
    
    @property
    def connection(self) -> amqpstorm.Connection:
        """获取连接实例"""
        with self._lock:
            if self._shutdown:
                raise RuntimeError("ConnectionManager has been shut down")
            
            if self._connection is None or not self._connection.is_open:
                self._connection = self._create_connection()
            return self._connection
    
    def create_channel(self, name: str, confirm_delivery: bool = True) -> amqpstorm.Channel:
        """
        创建或获取命名channel，包含重试机制。
        
        :param name: channel名称
        :param confirm_delivery: 是否开启消息确认
        :return: Channel实例
        """
        with self._lock:
            if self._shutdown:
                raise RuntimeError("ConnectionManager has been shut down")
            
            # 如果channel已存在且可用，直接返回
            if name in self._channels and self._channels[name].is_open:
                return self._channels[name]
            
            # 获取连接（避免重复获取锁）
            if self._connection is None or not self._connection.is_open:
                self._connection = self._create_connection()
            
            # 创建新的channel，包含重试机制
            attempts = 1
            max_attempts = 3
            last_exception = None
            
            while attempts <= max_attempts:
                try:
                    channel = self._connection.channel()
                    if confirm_delivery:
                        channel.confirm_deliveries()
                    self._channels[name] = channel
                    logger.info(f"Created channel '{name}'")
                    return channel
                except (AMQPConnectionError, AMQPChannelError) as exc:
                    last_exception = exc
                    logger.warning(f"Failed to create channel '{name}' (attempt {attempts}): {exc}")
                    # 连接可能已断开，重新创建连接
                    try:
                        if self._connection and self._connection.is_open:
                            self._connection.close()
                    except Exception:
                        pass
                    self._connection = None
                    
                    if attempts < max_attempts:
                        time.sleep(0.5 * attempts)  # 递增延迟
                        # 重新创建连接
                        self._connection = self._create_connection()
                        attempts += 1
                    else:
                        break
                except Exception as exc:
                    logger.error(f"Failed to create channel '{name}': {exc}")
                    raise
            
            # 如果所有重试都失败了，抛出最后一个异常
            if last_exception:
                raise last_exception
            else:
                raise RuntimeError(f"Failed to create channel '{name}' after {max_attempts} attempts")
    
    def get_channel(self, name: str) -> Optional[amqpstorm.Channel]:
        """
        获取指定名称的channel。
        
        :param name: channel名称
        :return: Channel实例或None
        """
        with self._lock:
            return self._channels.get(name)
    
    def close_channel(self, name: str) -> bool:
        """
        关闭指定名称的channel。
        
        :param name: channel名称
        :return: 是否成功关闭
        """
        with self._lock:
            if name in self._channels:
                channel = self._channels.pop(name)
                try:
                    if channel.is_open:
                        channel.close()
                    logger.info(f"Closed channel '{name}'")
                    return True
                except Exception as exc:
                    logger.warning(f"Error closing channel '{name}': {exc}")
                    return False
            return False
    
    def list_channels(self) -> list:
        """
        列出所有channel名称。
        
        :return: channel名称列表
        """
        with self._lock:
            return list(self._channels.keys())
    
    def listener(self, queue_name: str, channel_name: str = "consumer", no_ack: bool = False, **kwargs):
        """
        装饰器：在指定channel上监听队列。
        
        :param queue_name: 队列名称
        :param channel_name: channel名称
        :param no_ack: 是否自动确认
        :param kwargs: 其他参数
        """
        def wrapper(callback: Callable[[amqpstorm.Message], Any]):
            logger.info(f"ConnectionManager listener on channel '{channel_name}' for queue {queue_name}")
            
            def target():
                # 获取或创建指定的channel
                channel = self.create_channel(channel_name, confirm_delivery=False)
                
                # 声明队列
                channel.queue.declare(queue_name, durable=True)
                
                # 开始消费
                try:
                    channel.basic.qos(prefetch_count=1)
                    channel.basic.consume(queue=queue_name, callback=callback, no_ack=no_ack, **kwargs)
                    channel.start_consuming(to_tuple=False)
                except Exception as exc:
                    logger.error(f"Error in listener for {queue_name} on channel {channel_name}: {exc}")
                    raise
            
            thread = threading.Thread(target=target, daemon=True)
            thread.start()
            return thread
        
        return wrapper
    
    def shutdown(self):
        """
        关闭所有channel和连接。
        """
        with self._lock:
            self._shutdown = True
            
            # 关闭所有channel
            for name, channel in self._channels.items():
                try:
                    if channel.is_open:
                        channel.close()
                except Exception as exc:
                    logger.warning(f"Error closing channel '{name}': {exc}")
            self._channels.clear()
            
            # 关闭连接
            if self._connection:
                try:
                    if self._connection.is_open:
                        self._connection.close()
                except Exception as exc:
                    logger.warning(f"Error closing connection: {exc}")
                finally:
                    self._connection = None
    
    def __enter__(self):
        """上下文管理器入口"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器退出，自动清理资源"""
        self.shutdown()
        return False


# 全局工厂实例
_default_factory = None


def get_default_factory() -> RabbitConnectionFactory:
    """
    获取默认的连接工厂实例（单例模式）。
    
    :return: RabbitConnectionFactory实例
    """
    global _default_factory
    if _default_factory is None:
        _default_factory = RabbitConnectionFactory()
    return _default_factory


def create_rabbit_connection(**config) -> RabbitMQStore:
    """
    使用默认工厂创建RabbitMQ连接的便捷函数。
    
    :param config: 连接配置
    :return: RabbitMQStore实例
    """
    factory = get_default_factory()
    return factory.create_connection(**config)


def get_connection_manager(name: str = "default", **config) -> ConnectionManager:
    """
    使用默认工厂获取连接管理器的便捷函数。
    
    :param name: 连接名称
    :param config: 连接配置
    :return: ConnectionManager实例
    """
    factory = get_default_factory()
    return factory.get_connection_manager(name, **config)


# alias

useRabbitMQ = RabbitMQStore
useRabbitListener = RabbitListener
rabbitConnectionFactory = RabbitConnectionFactory
