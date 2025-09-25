#!/usr/bin/env python3
"""
RabbitMQ 弹性测试示例 - 验证"永不断线"特性

这个示例演示了 use-rabbitmq 库在各种网络故障和服务中断情况下的弹性恢复能力：
1. 连接断开后的自动重连
2. 消息发送失败时的重试机制
3. 消费者断线重连和消息处理恢复
4. 多channel环境下的故障恢复

测试方法：
1. 启动此脚本
2. 在运行过程中停止/重启 RabbitMQ 服务
3. 观察程序如何自动恢复并继续工作

注意：请确保 RabbitMQ 服务正在运行，然后可以通过以下方式测试：
- macOS: brew services stop rabbitmq / brew services start rabbitmq
- Docker: docker stop rabbitmq / docker start rabbitmq
- 系统服务: sudo systemctl stop rabbitmq-server / sudo systemctl start rabbitmq-server
"""

import logging
import threading
import time
from datetime import datetime
from typing import Dict, List
import signal
import sys

from use_rabbitmq import RabbitMQStore

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('rabbitmq_resilience_test.log')
    ]
)
logger = logging.getLogger(__name__)

class ResilienceMonitor:
    """弹性监控器 - 记录连接状态、重试次数、成功/失败统计"""
    
    def __init__(self):
        self.stats = {
            'connection_attempts': 0,
            'connection_successes': 0,
            'connection_failures': 0,
            'send_attempts': 0,
            'send_successes': 0,
            'send_failures': 0,
            'consume_attempts': 0,
            'consume_successes': 0,
            'consume_failures': 0,
            'messages_processed': 0,
            'last_success_time': None,
            'last_failure_time': None,
            'downtime_periods': []
        }
        self.lock = threading.Lock()
        
    def record_connection_attempt(self):
        with self.lock:
            self.stats['connection_attempts'] += 1
            
    def record_connection_success(self):
        with self.lock:
            self.stats['connection_successes'] += 1
            self.stats['last_success_time'] = datetime.now()
            
    def record_connection_failure(self):
        with self.lock:
            self.stats['connection_failures'] += 1
            self.stats['last_failure_time'] = datetime.now()
            
    def record_send_attempt(self):
        with self.lock:
            self.stats['send_attempts'] += 1
            
    def record_send_success(self):
        with self.lock:
            self.stats['send_successes'] += 1
            
    def record_send_failure(self):
        with self.lock:
            self.stats['send_failures'] += 1
            
    def record_message_processed(self):
        with self.lock:
            self.stats['messages_processed'] += 1
            
    def get_stats(self) -> Dict:
        with self.lock:
            return self.stats.copy()
            
    def print_stats(self):
        stats = self.get_stats()
        logger.info("=== 弹性测试统计 ===")
        logger.info(f"连接尝试: {stats['connection_attempts']}")
        logger.info(f"连接成功: {stats['connection_successes']}")
        logger.info(f"连接失败: {stats['connection_failures']}")
        logger.info(f"发送尝试: {stats['send_attempts']}")
        logger.info(f"发送成功: {stats['send_successes']}")
        logger.info(f"发送失败: {stats['send_failures']}")
        logger.info(f"消息处理: {stats['messages_processed']}")
        if stats['last_success_time']:
            logger.info(f"最后成功时间: {stats['last_success_time']}")
        if stats['last_failure_time']:
            logger.info(f"最后失败时间: {stats['last_failure_time']}")
        
        # 计算成功率
        if stats['connection_attempts'] > 0:
            conn_rate = (stats['connection_successes'] / stats['connection_attempts']) * 100
            logger.info(f"连接成功率: {conn_rate:.2f}%")
        if stats['send_attempts'] > 0:
            send_rate = (stats['send_successes'] / stats['send_attempts']) * 100
            logger.info(f"发送成功率: {send_rate:.2f}%")

class ResilienceTest:
    """弹性测试主类"""
    
    def __init__(self):
        self.monitor = ResilienceMonitor()
        self.running = True
        self.rabbitmq_store = None
        self.channels = {}  # 存储多个channel
        
    def setup_signal_handlers(self):
        """设置信号处理器，优雅退出"""
        def signal_handler(signum, frame):
            logger.info("收到退出信号，正在优雅关闭...")
            self.running = False
            
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
    def create_rabbitmq_connection(self) -> RabbitMQStore | None:
        """创建RabbitMQ连接，带重试和监控"""
        while self.running:
            try:
                self.monitor.record_connection_attempt()
                logger.info("尝试连接到 RabbitMQ...")
                
                store = RabbitMQStore(
                    host='localhost',
                    port=5672,
                    username='admin',
                    password='admin',
                )
                
                # 测试连接
                _ = store.connection
                logger.info("✅ RabbitMQ 连接成功!")
                self.monitor.record_connection_success()
                return store
                
            except Exception as e:
                logger.error(f"❌ RabbitMQ 连接失败: {e}")
                self.monitor.record_connection_failure()
                if self.running:
                    logger.info("5秒后重试连接...")
                    time.sleep(5)
                    
        return None
        
    def test_connection_recovery(self):
        """测试连接断开后的自动重连功能"""
        logger.info("🔄 开始测试连接恢复...")
        
        while self.running:
            try:
                if not self.rabbitmq_store:
                    self.rabbitmq_store = self.create_rabbitmq_connection()
                    if not self.rabbitmq_store:
                        continue
                        
                # 声明测试队列
                queue_name = "resilience_test_queue"
                self.rabbitmq_store.declare_queue(queue_name, durable=True)
                logger.info(f"✅ 队列 '{queue_name}' 声明成功")
                
                # 测试连接状态
                conn = self.rabbitmq_store.connection
                if conn and conn.is_open:
                    logger.info("✅ 连接状态正常")
                    self.monitor.record_connection_success()
                else:
                    logger.warning("⚠️ 连接状态异常，尝试重新连接...")
                    self.rabbitmq_store = None
                    continue
                    
                time.sleep(10)  # 每10秒检查一次连接状态
                
            except Exception as e:
                logger.error(f"❌ 连接测试失败: {e}")
                self.monitor.record_connection_failure()
                self.rabbitmq_store = None
                time.sleep(5)
                
    def test_send_retry(self):
        """测试消息发送失败时的重试机制"""
        logger.info("📤 开始测试发送重试...")
        
        message_counter = 0
        while self.running:
            try:
                if not self.rabbitmq_store:
                    time.sleep(1)
                    continue
                    
                message_counter += 1
                message = f"弹性测试消息 #{message_counter} - {datetime.now()}"
                queue_name = "resilience_test_queue"
                
                self.monitor.record_send_attempt()
                logger.info(f"📤 尝试发送消息: {message}")
                
                # 发送消息（内置重试机制）
                result = self.rabbitmq_store.send(queue_name, message)
                
                logger.info(f"✅ 消息发送成功: {result}")
                self.monitor.record_send_success()
                
                time.sleep(3)  # 每3秒发送一条消息
                
            except Exception as e:
                logger.error(f"❌ 消息发送失败: {e}")
                self.monitor.record_send_failure()
                # 发送失败时，重置连接
                self.rabbitmq_store = None
                time.sleep(2)
                
    def test_multi_channel_resilience(self):
        """测试多channel环境下的故障恢复"""
        logger.info("🔀 开始测试多channel弹性...")
        
        while self.running:
            try:
                if not self.rabbitmq_store:
                    time.sleep(1)
                    continue
                    
                # 创建多个channel
                if not self.channels:
                    for i in range(3):
                        channel_id = self.rabbitmq_store.create_channel()
                        self.channels[f"channel_{i}"] = channel_id
                        logger.info(f"✅ 创建channel: {channel_id}")
                        
                # 在不同channel上声明队列
                for name, channel_id in self.channels.items():
                    try:
                        queue_name = f"multi_channel_queue_{name}"
                        self.rabbitmq_store.declare_queue(
                            queue_name, 
                            durable=True, 
                            channel_id=channel_id
                        )
                        
                        # 发送测试消息
                        message = f"多channel消息 from {name} - {datetime.now()}"
                        self.rabbitmq_store.send(
                            queue_name, 
                            message, 
                            channel_id=channel_id
                        )
                        logger.info(f"✅ {name} 发送成功: {message}")
                        
                    except Exception as e:
                        logger.error(f"❌ {name} 操作失败: {e}")
                        # 移除失败的channel，让它重新创建
                        if name in self.channels:
                            del self.channels[name]
                            
                time.sleep(5)  # 每5秒测试一次多channel
                
            except Exception as e:
                logger.error(f"❌ 多channel测试失败: {e}")
                self.channels.clear()
                self.rabbitmq_store = None
                time.sleep(3)
                
    def test_consume_recovery(self):
        """测试消费者断线重连和消息处理恢复"""
        logger.info("📥 开始测试消费恢复...")
        
        def message_handler(message):
            try:
                content = message.body
                logger.info(f"📥 收到消息: {content}")
                self.monitor.record_message_processed()
                
                # 模拟消息处理
                time.sleep(0.1)
                
            except Exception as e:
                logger.error(f"❌ 消息处理失败: {e}")
                raise  # 重新抛出异常，让消息重新入队
                
        while self.running:
            try:
                if not self.rabbitmq_store:
                    time.sleep(1)
                    continue
                    
                queue_name = "resilience_test_queue"
                logger.info(f"📥 开始消费队列: {queue_name}")
                
                # 开始消费（这会阻塞）
                self.rabbitmq_store.start_consuming(
                    queue_name=queue_name,
                    callback=message_handler,
                    prefetch=1
                )
                
            except Exception as e:
                logger.error(f"❌ 消费失败: {e}")
                self.rabbitmq_store = None
                time.sleep(5)
                logger.info("🔄 尝试重新开始消费...")
                
    def run_monitoring_thread(self):
        """运行监控线程，定期打印统计信息"""
        while self.running:
            time.sleep(30)  # 每30秒打印一次统计
            if self.running:
                self.monitor.print_stats()
                
    def run_test(self):
        """运行完整的弹性测试"""
        logger.info("🚀 开始 RabbitMQ 弹性测试")
        logger.info("💡 测试提示：在运行过程中可以停止/重启 RabbitMQ 服务来测试弹性恢复")
        logger.info("💡 停止测试：按 Ctrl+C")
        
        self.setup_signal_handlers()
        
        # 启动各种测试线程
        threads = [
            threading.Thread(target=self.test_connection_recovery, name="ConnectionTest"),
            threading.Thread(target=self.test_send_retry, name="SendTest"),
            threading.Thread(target=self.test_multi_channel_resilience, name="MultiChannelTest"),
            threading.Thread(target=self.test_consume_recovery, name="ConsumeTest"),
            threading.Thread(target=self.run_monitoring_thread, name="MonitoringThread"),
        ]
        
        # 启动所有线程
        for thread in threads:
            thread.daemon = True
            thread.start()
            logger.info(f"✅ 启动线程: {thread.name}")
            
        try:
            # 主线程等待
            while self.running:
                time.sleep(1)
                
        except KeyboardInterrupt:
            logger.info("收到中断信号...")
            
        finally:
            logger.info("🛑 停止弹性测试...")
            self.running = False
            
            # 等待线程结束
            for thread in threads:
                if thread.is_alive():
                    thread.join(timeout=5)
                    
            # 打印最终统计
            self.monitor.print_stats()
            
            # 清理资源
            if self.rabbitmq_store:
                try:
                    self.rabbitmq_store.shutdown()
                    logger.info("✅ RabbitMQ 连接已关闭")
                except Exception as e:
                    logger.error(f"❌ 关闭连接时出错: {e}")
                    
            logger.info("🏁 弹性测试结束")

def main():
    """主函数"""
    print("=" * 60)
    print("🐰 RabbitMQ 弹性测试 - 验证'永不断线'特性")
    print("=" * 60)
    print()
    print("📋 测试内容:")
    print("  1. 连接断开后的自动重连")
    print("  2. 消息发送失败时的重试机制")
    print("  3. 多channel环境下的故障恢复")
    print("  4. 消费者断线重连和消息处理恢复")
    print()
    print("🧪 测试方法:")
    print("  1. 启动此脚本")
    print("  2. 在运行过程中停止/重启 RabbitMQ 服务")
    print("  3. 观察程序如何自动恢复并继续工作")
    print()
    print("⚡ RabbitMQ 服务控制命令:")
    print("  macOS:   brew services stop/start rabbitmq")
    print("  Docker:  docker stop/start rabbitmq")
    print("  Linux:   sudo systemctl stop/start rabbitmq-server")
    print()
    print("📊 日志文件: rabbitmq_resilience_test.log")
    print("🛑 停止测试: 按 Ctrl+C")
    print("=" * 60)
    print()
    
    # 等待用户确认
    try:
        input("按 Enter 键开始测试...")
    except KeyboardInterrupt:
        print("\n测试已取消")
        return
        
    # 运行测试
    test = ResilienceTest()
    test.run_test()

if __name__ == "__main__":
    main()
