import logging
import os
import threading
import time
import uuid
from typing import Callable, Optional, Union, Any, Dict, List
from collections import defaultdict
from contextlib import contextmanager
from queue import Queue, Empty

import amqpstorm
from amqpstorm import Message
from amqpstorm.exception import AMQPConnectionError, AMQPChannelError

logger = logging.getLogger(__name__)


class ConnectionPool:
    """
    RabbitMQ连接池管理器
    
    提供连接复用、自动清理和负载均衡功能，提高性能和资源利用率。
    支持多个RabbitMQStore实例共享连接池。
    """
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if hasattr(self, '_initialized'):
            return
        self._initialized = True
        self._pools = defaultdict(Queue)  # 按连接参数分组的连接池
        self._pool_locks = defaultdict(threading.Lock)
        self._connection_counts = defaultdict(int)
        self._max_connections = 10  # 每个池的最大连接数
        self._cleanup_lock = threading.Lock()
    
    def _get_pool_key(self, parameters: Dict[str, Any]) -> str:
        """根据连接参数生成池键"""
        key_params = {
            'hostname': parameters.get('hostname', 'localhost'),
            'port': parameters.get('port', 5672),
            'username': parameters.get('username', 'guest'),
            'virtual_host': parameters.get('virtual_host', '/')
        }
        return str(sorted(key_params.items()))
    
    def get_connection(self, parameters: Dict[str, Any], client_name: Optional[str] = None) -> amqpstorm.Connection:
        """从连接池获取连接"""
        pool_key = self._get_pool_key(parameters)
        pool = self._pools[pool_key]
        pool_lock = self._pool_locks[pool_key]
        
        with pool_lock:
            # 尝试从池中获取可用连接
            while not pool.empty():
                try:
                    connection = pool.get_nowait()
                    if connection.is_open:
                        logger.debug(f"从连接池获取连接: {pool_key}")
                        return connection
                    else:
                        # 连接已关闭，减少计数
                        self._connection_counts[pool_key] -= 1
                except Empty:
                    break
            
            # 池中没有可用连接，创建新连接
            if self._connection_counts[pool_key] < self._max_connections:
                try:
                    # 添加客户端名称支持
                    conn_params = parameters.copy()
                    if client_name:
                        conn_params['client_properties'] = {
                            'connection_name': client_name,
                            'client_name': client_name,
                            'created_at': time.strftime('%Y-%m-%d %H:%M:%S')
                        }
                    
                    connection = amqpstorm.Connection(**conn_params)
                    self._connection_counts[pool_key] += 1
                    logger.debug(f"创建新连接: {pool_key}, 当前连接数: {self._connection_counts[pool_key]}")
                    return connection
                except Exception as e:
                    logger.error(f"创建连接失败: {e}")
                    raise
            else:
                raise RuntimeError(f"连接池已满，最大连接数: {self._max_connections}")
    
    def return_connection(self, connection: amqpstorm.Connection, parameters: Dict[str, Any]):
        """将连接返回到池中"""
        if not connection.is_open:
            pool_key = self._get_pool_key(parameters)
            with self._pool_locks[pool_key]:
                self._connection_counts[pool_key] -= 1
            return
        
        pool_key = self._get_pool_key(parameters)
        pool = self._pools[pool_key]
        pool_lock = self._pool_locks[pool_key]
        
        with pool_lock:
            if pool.qsize() < self._max_connections:
                pool.put(connection)
                logger.debug(f"连接返回到池: {pool_key}")
            else:
                # 池已满，关闭连接
                try:
                    connection.close()
                except Exception as e:
                    logger.warning(f"关闭多余连接时出错: {e}")
                self._connection_counts[pool_key] -= 1
    
    def cleanup_pool(self, pool_key: Optional[str] = None):
        """清理连接池"""
        with self._cleanup_lock:
            if pool_key:
                self._cleanup_single_pool(pool_key)
            else:
                # 清理所有池
                for key in list(self._pools.keys()):
                    self._cleanup_single_pool(key)
    
    def _cleanup_single_pool(self, pool_key: str):
        """清理单个连接池"""
        pool = self._pools[pool_key]
        pool_lock = self._pool_locks[pool_key]
        
        with pool_lock:
            while not pool.empty():
                try:
                    connection = pool.get_nowait()
                    if connection.is_open:
                        connection.close()
                    self._connection_counts[pool_key] -= 1
                except Empty:
                    break
                except Exception as e:
                    logger.warning(f"清理连接时出错: {e}")
            
            # 清理空的数据结构
            if self._connection_counts[pool_key] <= 0:
                del self._pools[pool_key]
                del self._pool_locks[pool_key]
                del self._connection_counts[pool_key]
    
    def get_pool_stats(self) -> Dict[str, Dict[str, int]]:
        """获取连接池统计信息"""
        stats = {}
        for pool_key in self._pools.keys():
            with self._pool_locks[pool_key]:
                stats[pool_key] = {
                    'total_connections': self._connection_counts[pool_key],
                    'available_connections': self._pools[pool_key].qsize(),
                    'active_connections': self._connection_counts[pool_key] - self._pools[pool_key].qsize()
                }
        return stats


class ChannelManager:
    """
    单连接多通道管理器
    
    为单个连接管理多个通道，提高资源利用率和性能。
    支持通道复用、自动清理和故障恢复。
    """
    
    def __init__(self, connection: amqpstorm.Connection, max_channels: int = 20):
        self.connection = connection
        self.max_channels = max_channels
        self._channels = Queue()
        self._channel_count = 0
        self._lock = threading.Lock()
        self._confirm_delivery = True
    
    def set_confirm_delivery(self, confirm_delivery: bool):
        """设置消息确认模式"""
        self._confirm_delivery = confirm_delivery
    
    @contextmanager
    def get_channel(self):
        """获取通道的上下文管理器"""
        channel = self._acquire_channel()
        try:
            yield channel
        finally:
            self._release_channel(channel)
    
    def _acquire_channel(self) -> amqpstorm.Channel:
        """获取通道"""
        with self._lock:
            # 尝试从池中获取可用通道
            while not self._channels.empty():
                try:
                    channel = self._channels.get_nowait()
                    if channel.is_open:
                        return channel
                    else:
                        self._channel_count -= 1
                except Empty:
                    break
            
            # 创建新通道
            if self._channel_count < self.max_channels:
                if not self.connection.is_open:
                    raise AMQPConnectionError("连接已关闭")
                
                channel = self.connection.channel()
                if self._confirm_delivery:
                    channel.confirm_deliveries()
                self._channel_count += 1
                return channel
            else:
                raise RuntimeError(f"通道池已满，最大通道数: {self.max_channels}")
    
    def _release_channel(self, channel: amqpstorm.Channel):
        """释放通道"""
        if not channel.is_open:
            with self._lock:
                self._channel_count -= 1
            return
        
        with self._lock:
            if self._channels.qsize() < self.max_channels:
                self._channels.put(channel)
            else:
                # 池已满，关闭通道
                try:
                    channel.close()
                except Exception as e:
                    logger.warning(f"关闭多余通道时出错: {e}")
                self._channel_count -= 1
    
    def cleanup(self):
        """清理所有通道"""
        with self._lock:
            while not self._channels.empty():
                try:
                    channel = self._channels.get_nowait()
                    if channel.is_open:
                        channel.close()
                    self._channel_count -= 1
                except Empty:
                    break
                except Exception as e:
                    logger.warning(f"清理通道时出错: {e}")
    
    def get_stats(self) -> Dict[str, int]:
        """获取通道管理器统计信息"""
        with self._lock:
            return {
                'total_channels': self._channel_count,
                'available_channels': self._channels.qsize(),
                'active_channels': self._channel_count - self._channels.qsize()
            }


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
            client_name: Optional[str] = None,
            use_connection_pool: bool = True,
            use_channel_manager: bool = True,
            max_channels: int = 20,
            **kwargs,
    ):
        """
        :param confirm_delivery: 是否开启消息确认
        :param host: RabbitMQ host
        :param port: RabbitMQ port
        :param username: RabbitMQ username
        :param password: RabbitMQ password
        :param client_name: 客户端自定义名称，用于监控和调试
        :param use_connection_pool: 是否使用连接池管理
        :param use_channel_manager: 是否使用多通道管理器
        :param max_channels: 最大通道数（仅在use_channel_manager=True时有效）
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
        
        # 核心配置
        self.confirm_delivery = confirm_delivery
        self.client_name = client_name or f"use-rabbitmq-{uuid.uuid4().hex[:8]}"
        self.use_connection_pool = use_connection_pool
        self.use_channel_manager = use_channel_manager
        self.max_channels = max_channels
        
        # 连接和通道管理
        self._connection: Optional[amqpstorm.Connection] = None
        self._channel: Optional[amqpstorm.Channel] = None
        self._connection_pool: Optional[ConnectionPool] = None
        self._channel_manager: Optional[ChannelManager] = None
        
        # 初始化连接池（如果启用）
        if self.use_connection_pool:
            self._connection_pool = ConnectionPool()
        
        logger.info(f"RabbitMQStore初始化完成，客户端名称: {self.client_name}")

    def _create_connection(self) -> amqpstorm.Connection:
        # 如果启用连接池，从池中获取连接
        if self.use_connection_pool and self._connection_pool:
            try:
                return self._connection_pool.get_connection(self.parameters, self.client_name)
            except Exception as exc:
                logger.warning(f"从连接池获取连接失败，回退到直接创建: {exc}")
        
        # 直接创建连接（原有逻辑）
        attempts = 1
        reconnection_delay = self.RECONNECTION_DELAY
        while attempts <= self.MAX_CONNECTION_ATTEMPTS:
            try:
                # 添加客户端名称支持
                conn_params = self.parameters.copy()
                conn_params['client_properties'] = {
                    'connection_name': self.client_name,
                    'client_name': self.client_name,
                    'created_at': time.strftime('%Y-%m-%d %H:%M:%S')
                }
                
                connector = amqpstorm.Connection(**conn_params)
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
                
                # 如果启用通道管理器，初始化它
                if self.use_channel_manager:
                    if self._channel_manager:
                        self._channel_manager.cleanup()
                    self._channel_manager = ChannelManager(self._connection, self.max_channels)
                    self._channel_manager.set_confirm_delivery(self.confirm_delivery)
                    
            return self._connection

    @connection.deleter
    def connection(self) -> None:
        with self._lock:
            # 清理通道管理器
            if self._channel_manager:
                try:
                    self._channel_manager.cleanup()
                except Exception as exc:
                    logger.exception(f"RabbitmqStore channel manager cleanup error<{exc}>")
                finally:
                    self._channel_manager = None
            
            # 先删除channel（兼容原有逻辑）
            if self._channel:
                try:
                    if self._channel.is_open:
                        self._channel.close()
                except Exception as exc:
                    logger.exception(f"RabbitmqStore channel close error<{exc}>")
                finally:
                    self._channel = None
            
            # 处理connection
            if self._connection:
                try:
                    # 如果使用连接池，将连接返回到池中
                    if self.use_connection_pool and self._connection_pool:
                        self._connection_pool.return_connection(self._connection, self.parameters)
                    else:
                        # 否则直接关闭连接
                        if self._connection.is_open:
                            self._connection.close()
                except Exception as exc:
                    logger.exception(f"RabbitmqStore connection close error<{exc}>")
                finally:
                    self._connection = None

    @property
    def channel(self) -> amqpstorm.Channel:
        # 如果启用通道管理器，使用管理器获取通道
        if self.use_channel_manager:
            # 确保连接可用
            _ = self.connection  # 这会触发连接和通道管理器的初始化
            if self._channel_manager:
                return self._channel_manager._acquire_channel()
            else:
                raise RuntimeError("通道管理器未初始化")
        
        # 原有的单通道逻辑（向后兼容）
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

    @contextmanager
    def get_channel(self):
        """获取通道的上下文管理器（推荐用于多通道模式）"""
        if self.use_channel_manager and self._channel_manager:
            with self._channel_manager.get_channel() as channel:
                yield channel
        else:
            # 回退到普通通道
            channel = self.channel
            try:
                yield channel
            finally:
                # 在非通道管理器模式下，不需要特殊清理
                pass
    
    @channel.deleter
    def channel(self):
        # 注意：在通道管理器模式下，不建议直接删除通道
        # 应该使用 get_channel() 上下文管理器
        if self.use_channel_manager:
            logger.warning("在通道管理器模式下不建议直接删除通道，请使用 get_channel() 上下文管理器")
            return
            
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
                
                # 清理通道管理器
                if self._channel_manager:
                    try:
                        self._channel_manager.cleanup()
                    except Exception as exc:
                        logger.warning(f"Error cleaning up channel manager during shutdown: {exc}")
                    finally:
                        self._channel_manager = None
                
                # 关闭通道（兼容原有逻辑）
                if self._channel:
                    try:
                        if self._channel.is_open:
                            self._channel.close()
                    except Exception as exc:
                        logger.warning(f"Error closing channel during shutdown: {exc}")
                    finally:
                        self._channel = None
                
                # 处理连接
                if self._connection:
                    try:
                        # 如果使用连接池，将连接返回到池中
                        if self.use_connection_pool and self._connection_pool:
                            self._connection_pool.return_connection(self._connection, self.parameters)
                        else:
                            # 否则直接关闭连接
                            if self._connection.is_open:
                                self._connection.close()
                    except Exception as exc:
                        logger.warning(f"Error closing connection during shutdown: {exc}")
                    finally:
                        self._connection = None
                        
            except Exception as exc:
                logger.error(f"Error during shutdown: {exc}")
        
        logger.info(f"RabbitMQStore客户端 {self.client_name} 已安全关闭")
    
    def get_connection_pool_stats(self) -> Optional[Dict[str, Dict[str, int]]]:
        """获取连接池统计信息"""
        if self.use_connection_pool and self._connection_pool:
            return self._connection_pool.get_pool_stats()
        return None
    
    def get_channel_manager_stats(self) -> Optional[Dict[str, int]]:
        """获取通道管理器统计信息"""
        if self.use_channel_manager and self._channel_manager:
            return self._channel_manager.get_stats()
        return None
    
    def get_client_info(self) -> Dict[str, Any]:
        """获取客户端信息"""
        return {
            'client_name': self.client_name,
            'use_connection_pool': self.use_connection_pool,
            'use_channel_manager': self.use_channel_manager,
            'max_channels': self.max_channels,
            'confirm_delivery': self.confirm_delivery,
            'connection_parameters': {
                'hostname': self.parameters.get('hostname'),
                'port': self.parameters.get('port'),
                'username': self.parameters.get('username'),
                'virtual_host': self.parameters.get('virtual_host', '/')
            },
            'connection_pool_stats': self.get_connection_pool_stats(),
            'channel_manager_stats': self.get_channel_manager_stats()
        }

    def declare_queue(self, queue_name: str, durable: bool = True, **kwargs):
        """声明队列
        
        Args:
            queue_name: 队列名称
            durable: 是否持久化
            **kwargs: 其他队列声明参数
            
        Returns:
            队列声明结果，如果队列已存在则返回 None
        """
        return self._execute_queue_operation(
            lambda channel: self._declare_queue_with_channel(channel, queue_name, durable, **kwargs)
        )
    
    def _declare_queue_with_channel(self, channel, queue_name: str, durable: bool = True, **kwargs):
        """使用指定通道声明队列"""
        try:
            # 首先尝试被动声明（检查队列是否存在）
            channel.queue.declare(queue_name, passive=True, durable=durable)
            return None  # 队列已存在
        except amqpstorm.AMQPChannelError as exc:
            if exc.error_code != 404:
                raise exc
            # 队列不存在，创建队列
            return channel.queue.declare(queue_name, durable=durable, **kwargs)
    
    def _execute_queue_operation(self, operation):
        """执行队列操作的通用方法，处理通道管理和错误恢复"""
        if self.use_channel_manager:
            return self._execute_with_channel_manager(operation)
        else:
            return self._execute_with_traditional_channel(operation)
    
    def _execute_with_channel_manager(self, operation):
        """使用通道管理器执行操作"""
        try:
            with self.get_channel() as channel:
                return operation(channel)
        except amqpstorm.AMQPChannelError as exc:
            if exc.error_code == 404:
                # 通道因404错误关闭，使用新通道重试
                with self.get_channel() as new_channel:
                    return operation(new_channel)
            raise exc
    
    def _execute_with_traditional_channel(self, operation):
        """使用传统通道执行操作"""
        try:
            return operation(self.channel)
        except amqpstorm.AMQPChannelError as exc:
            if exc.error_code == 404:
                # 通道因404错误关闭，重新创建通道并重试
                del self.channel  # 强制重新创建通道
                return operation(self.channel)
            raise exc

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
                # 如果启用通道管理器，使用上下文管理器
                if self.use_channel_manager:
                    with self.get_channel() as channel:
                        channel.basic.publish(
                            message, queue_name, properties=properties, **kwargs
                        )
                else:
                    # 原有逻辑
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
        if self.use_channel_manager:
            with self.get_channel() as channel:
                channel.queue.purge(queue_name)
        else:
            self.channel.queue.purge(queue_name)

    def get_message_counts(self, queue_name: str) -> int:
        """获取消息数量"""
        if self.use_channel_manager:
            with self.get_channel() as channel:
                queue_response = channel.queue.declare(
                    queue_name, passive=True, durable=False
                )
                return queue_response.get("message_count", 0)
        else:
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


# alias

useRabbitMQ = RabbitMQStore
useRabbitListener = RabbitListener
