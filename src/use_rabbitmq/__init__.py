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
        del self.channel
        if self._connection:
            if self._connection.is_open:
                try:
                    self._connection.close()
                except Exception as exc:
                    logger.exception(f"RabbitmqStore connection close error<{exc}>")
            self._connection = None

    @property
    def channel(self) -> amqpstorm.Channel:
        with self._lock:
            if all([self._connection, self._channel]) and all(
                    [self._connection.is_open, self._channel.is_open]
            ):
                return self._channel
            self._channel = self.connection.channel()
            if self.confirm_delivery:
                self._channel.confirm_deliveries()
            return self._channel

    @channel.deleter
    def channel(self):
        if self._channel:
            if self._channel.is_open:
                try:
                    self._channel.close()
                except Exception as exc:
                    logger.exception(f"RabbitmqStore channel close error<{exc}>")
            self._channel = None

    def shutdown(self):
        self.__shutdown = True
        del self.connection

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
                logger.error(f"RabbitmqStore channel error: {exc}")
                raise exc
            except AMQPConnectionError as exc:
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
                if self.__shutdown:
                    break

    def __del__(self):
        self.shutdown()

    def listener(self, queue_name: str, no_ack: bool = False, **kwargs):
        self.declare_queue(queue_name)

        def wrapper(callback: Callable[[Message], Any]):
            logger.info(f"RabbitMQStore consume {queue_name}")

            def target():
                self.start_consuming(queue_name, callback, no_ack=no_ack, **kwargs)

            thread = threading.Thread(target=target)
            thread.start()
            return thread

        return wrapper

    def stop_listener(self, queue_name: str) -> None:
        """停止监听指定队列"""
        try:
            if self._channel and self._channel.is_open:
                self.channel.basic.cancel(queue_name)
        except Exception as exc:
            logger.warning(f"Error canceling consumer for queue {queue_name}: {exc}")
        finally:
            self.shutdown()


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
