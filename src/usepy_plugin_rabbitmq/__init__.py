import logging
import time

import amqpstorm
from amqpstorm.exception import AMQPConnectionError

logger = logging.Logger(__name__)


class RabbitMQStore:
    MAX_SEND_ATTEMPTS = 6  # 最大发送重试次数
    MAX_CONNECTION_ATTEMPTS = float('inf')  # 最大连接重试次数
    MAX_CONNECTION_DELAY = 2 ** 5  # 最大延迟时间
    RECONNECTION_DELAY = 1

    def __init__(self, *, confirm_delivery=True, host=None, port=None, username=None, password=None,
                 **kwargs):
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
            'hostname': host or 'localhost',
            'port': port or 5672,
            'username': username or 'guest',
            'password': password or 'guest',
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
                    logger.warning(f"RabbitmqStore connection succeeded after {attempts} attempts", )
                return connector
            except AMQPConnectionError as exc:
                logger.warning(f"RabbitmqStore connection error<{exc}>; retrying in {reconnection_delay} seconds")
                attempts += 1
                time.sleep(reconnection_delay)
                reconnection_delay = min(reconnection_delay * 2, self.MAX_CONNECTION_DELAY)
        raise AMQPConnectionError("RabbitmqStore connection error, max attempts reached")

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
        if all([self._connection, self._channel]) and all([self._connection.is_open, self._channel.is_open]):
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

    def declare_queue(self, queue_name, arguments=None):
        """声明队列"""
        return self.channel.queue.declare(queue_name, durable=True, arguments=arguments)

    def send(self, queue_name, message, priority=None, **kwargs):
        """发送消息"""
        attempts = 1
        while True:
            try:
                self.declare_queue(queue_name)
                self.channel.basic.publish(
                    message, queue_name, properties=priority, **kwargs
                )
                return message
            except Exception as exc:
                del self.connection
                attempts += 1
                if attempts > self.MAX_SEND_ATTEMPTS:
                    raise exc

    def flush_queue(self, queue_name):
        """清空队列"""
        self.channel.queue.purge(queue_name)

    def get_message_counts(self, queue_name: str) -> int:
        """获取消息数量"""
        queue_response = self.declare_queue(queue_name)
        return queue_response.get("message_count", 0)

    def start_consuming(self, queue_name, callback, prefetch=1, **kwargs):
        """开始消费"""
        self.__shutdown = False
        reconnection_delay = self.RECONNECTION_DELAY
        while not self.__shutdown:
            try:
                self.channel.basic.qos(prefetch_count=prefetch)
                self.channel.basic.consume(queue=queue_name, callback=callback, no_ack=False, **kwargs)
                self.channel.start_consuming(to_tuple=False)
            except AMQPConnectionError:
                logger.warning("RabbitmqStore consume connection error, reconnecting...")
                del self.connection
                time.sleep(reconnection_delay)
                reconnection_delay = min(reconnection_delay * 2, self.MAX_CONNECTION_DELAY)
            except Exception as e:
                if self.__shutdown:
                    break
                logger.exception(f"RabbitmqStore consume error<{e}>, reconnecting...")
                del self.connection
                time.sleep(reconnection_delay)
                reconnection_delay = min(reconnection_delay * 2, self.MAX_CONNECTION_DELAY)
            finally:
                if self.__shutdown:
                    break

    def __del__(self):
        self.shutdown()


useRabbitMQ = RabbitMQStore
