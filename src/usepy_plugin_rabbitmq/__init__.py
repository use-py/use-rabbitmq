import logging
import time
from typing import Optional, Union, Callable

import amqpstorm
from amqpstorm.exception import AMQPConnectionError

logger = logging.getLogger(__name__)


class RabbitMQStore:
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
        self.parameters = {
            "hostname": host or "localhost",
            "port": port or 5672,
            "username": username or "guest",
            "password": password or "guest",
        }
        if kwargs:
            self.parameters.update(kwargs)
        self.confirm_delivery = confirm_delivery
        self._connection = None
        self._channel = None

    def _create_connection(self):
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
        if self._connection is None or not self._connection.is_open:
            self._connection = self._create_connection()
        return self._connection

    @connection.deleter
    def connection(self):
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

    def send(self, queue_name: str, message: Union[str, bytes], priority: Optional[dict] = None, **kwargs):
        """发送消息"""
        attempts = 1
        while True:
            try:
                self.channel.basic.publish(
                    message, queue_name, properties=priority, **kwargs
                )
                return message
            except Exception as exc:
                del self.connection
                attempts += 1
                if attempts > self.MAX_SEND_ATTEMPTS:
                    raise exc

    def flush_queue(self, queue_name: str):
        """清空队列"""
        self.channel.queue.purge(queue_name)

    def get_message_counts(self, queue_name: str) -> int:
        """获取消息数量"""
        queue_response = self.channel.queue.declare(
            queue_name, passive=True, durable=False
        )
        return queue_response.get("message_count", 0)

    def start_consuming(self, queue_name: str, callback: Callable, prefetch=1, **kwargs):
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
            except AMQPConnectionError as exc:
                logger.warning(
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

        def wrapper(callback):
            logger.info(f"RabbitMQStore consume {queue_name}")
            self.start_consuming(queue_name, callback, no_ack=no_ack, **kwargs)

        return wrapper

    def stop_listener(self, queue_name: str):
        self.channel.basic.cancel(queue_name)
        self.shutdown()


useRabbitMQ = RabbitMQStore


def useRabbitListener(instance: RabbitMQStore, *, queue_name: str, no_ack: bool = False, **kwargs):
    return instance.listener(queue_name, no_ack=no_ack, **kwargs)
