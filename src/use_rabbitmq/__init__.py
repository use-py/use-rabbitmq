import logging
import os
import threading
import time
from typing import Callable, Optional, Union, Any, Dict
import uuid

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
        
    支持多channel管理:
        # 获取新的channel
        channel_id = mq.create_channel()
        # 使用指定channel发送消息
        mq.send('queue', 'message', channel_id=channel_id)
        # 释放channel
        mq.close_channel(channel_id)
        
    支持客户端名称设置:
        # 设置客户端名称用于连接标识
        mq = RabbitMQStore(client_name="my-app-v1.0")
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
            **kwargs,
    ):
        """
        :param confirm_delivery: 是否开启消息确认
        :param host: RabbitMQ host
        :param port: RabbitMQ port
        :param username: RabbitMQ username
        :param password: RabbitMQ password
        :param client_name: 客户端名称，用于标识连接，默认为None时会自动生成
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
        # 添加client_name到连接参数中
        if client_name is None:
            # 生成默认的客户端名称，包含随机后缀
            client_name = f"use-rabbitmq-client-{uuid.uuid4().hex[:5]}"
        self.parameters["client_properties"] = {"connection_name": client_name}
        if kwargs:
            self.parameters.update(kwargs)
        self.confirm_delivery = confirm_delivery
        self._connection: Optional[amqpstorm.Connection] = None
        self._channel: Optional[amqpstorm.Channel] = None
        
        # 多channel管理
        self._channels: Dict[str, amqpstorm.Channel] = {}
        self._channel_lock = threading.Lock()

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
            # 先删除所有channels
            with self._channel_lock:
                for channel_id, channel in list(self._channels.items()):
                    try:
                        if channel and channel.is_open:
                            channel.close()
                    except Exception as exc:
                        logger.exception(f"RabbitmqStore channel {channel_id} close error<{exc}>")
                    finally:
                        del self._channels[channel_id]
            
            # 删除默认channel
            if self._channel:
                try:
                    if self._channel.is_open:
                        self._channel.close()
                except Exception as exc:
                    logger.exception(f"RabbitmqStore default channel close error<{exc}>")
                finally:
                    self._channel = None
            
            # 删除连接
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

    def create_channel(self) -> str:
        """
        创建一个新的channel并返回channel_id
        
        :return: channel_id
        """
        with self._channel_lock:
            channel_id = str(uuid.uuid4())
            try:
                # 确保连接可用
                connection = self.connection
                channel = connection.channel()
                if self.confirm_delivery:
                    channel.confirm_deliveries()
                self._channels[channel_id] = channel
                logger.debug(f"Created new channel: {channel_id}")
                return channel_id
            except Exception as exc:
                logger.exception(f"Failed to create channel: {exc}")
                raise

    def get_channel(self, channel_id: Optional[str] = None) -> amqpstorm.Channel:
        """
        获取指定的channel，如果channel_id为None则返回默认channel
        
        :param channel_id: channel ID，如果为None则使用默认channel
        :return: amqpstorm.Channel
        """
        if channel_id is None:
            return self.channel
        
        with self._channel_lock:
            if channel_id not in self._channels:
                raise ValueError(f"Channel {channel_id} not found")
            
            channel = self._channels[channel_id]
            if not channel.is_open:
                # 重新创建channel
                try:
                    connection = self.connection
                    new_channel = connection.channel()
                    if self.confirm_delivery:
                        new_channel.confirm_deliveries()
                    self._channels[channel_id] = new_channel
                    logger.debug(f"Recreated channel: {channel_id}")
                    return new_channel
                except Exception as exc:
                    logger.exception(f"Failed to recreate channel {channel_id}: {exc}")
                    raise
            
            return channel

    def close_channel(self, channel_id: str) -> None:
        """
        关闭并移除指定的channel
        
        :param channel_id: channel ID
        """
        with self._channel_lock:
            if channel_id in self._channels:
                channel = self._channels[channel_id]
                try:
                    if channel.is_open:
                        channel.close()
                except Exception as exc:
                    logger.exception(f"Error closing channel {channel_id}: {exc}")
                finally:
                    del self._channels[channel_id]
                    logger.debug(f"Closed channel: {channel_id}")

    def list_channels(self) -> Dict[str, bool]:
        """
        列出所有channel及其状态
        
        :return: {channel_id: is_open}
        """
        with self._channel_lock:
            result = {}
            for channel_id, channel in self._channels.items():
                result[channel_id] = channel.is_open
            return result

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

    def declare_queue(self, queue_name: str, durable: bool = True, channel_id: Optional[str] = None, **kwargs):
        """声明队列
        
        :param queue_name: 队列名称
        :param durable: 是否持久化
        :param channel_id: 指定使用的channel ID，如果为None则使用默认channel
        :param kwargs: 其他参数
        """
        channel = self.get_channel(channel_id)
        try:
            channel.queue.declare(queue_name, passive=True, durable=durable)
        except amqpstorm.AMQPChannelError as exc:
            if exc.error_code != 404:
                raise exc
            channel = self.get_channel(channel_id)
            return channel.queue.declare(queue_name, durable=durable, **kwargs)

    def send(
            self,
            queue_name: str,
            message: Union[str, bytes],
            properties: Optional[dict] = None,
            channel_id: Optional[str] = None,
            **kwargs,
    ) -> Union[str, bytes]:
        """发送消息
        
        :param queue_name: 队列名称
        :param message: 消息内容
        :param properties: 消息属性
        :param channel_id: 指定使用的channel ID，如果为None则使用默认channel
        :param kwargs: 其他参数
        :return: 发送的消息
        """
        attempts = 1
        while True:
            try:
                channel = self.get_channel(channel_id)
                channel.basic.publish(
                    message, queue_name, properties=properties, **kwargs
                )
                return message
            except Exception as exc:
                del self.connection
                attempts += 1
                if attempts > self.MAX_SEND_ATTEMPTS:
                    raise exc
                time.sleep(self.RECONNECTION_DELAY)

    def flush_queue(self, queue_name: str, channel_id: Optional[str] = None):
        """清空队列
        
        :param queue_name: 队列名称
        :param channel_id: 指定使用的channel ID，如果为None则使用默认channel
        """
        channel = self.get_channel(channel_id)
        channel.queue.purge(queue_name)

    def get_message_counts(self, queue_name: str, channel_id: Optional[str] = None) -> int:
        """获取队列中的消息数量
        
        :param queue_name: 队列名称
        :param channel_id: 指定使用的channel ID，如果为None则使用默认channel
        :return: 消息数量
        """
        channel = self.get_channel(channel_id)
        result = channel.queue.declare(queue_name, passive=True, durable=False)
        return result["message_count"]

    def start_consuming(
            self, queue_name: str, callback: Callable, prefetch=1, channel_id: Optional[str] = None, **kwargs
    ):
        """开始消费消息
        
        :param queue_name: 队列名称
        :param callback: 回调函数
        :param prefetch: 预取数量
        :param channel_id: 指定使用的channel ID，如果为None则使用默认channel
        :param kwargs: 其他参数
        """
        self.__shutdown = False
        no_ack = kwargs.pop("no_ack", False)
        reconnection_delay = self.RECONNECTION_DELAY
        
        while not self.__shutdown:
            try:
            
                channel = self.get_channel(channel_id)

                # 设置预取数量
                channel.basic.qos(prefetch_count=prefetch)

                # 开始消费
                channel.basic.consume(
                    callback=callback,
                    queue=queue_name,
                    no_ack=no_ack,
                    **kwargs
                )
                # 启动消费循环
                channel.start_consuming(to_tuple=False)

            except amqpstorm.AMQPChannelError as exc:
                if self.__shutdown:
                    break
                logger.error(f"RabbitmqStore channel error: {exc}")
                del channel
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
