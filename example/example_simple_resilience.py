#!/usr/bin/env python3
"""
简化版 RabbitMQ 弹性测试

这是一个简单易懂的弹性测试示例，专门用来验证 use-rabbitmq 的"永不断线"特性。

测试步骤：
1. 运行此脚本
2. 脚本会持续发送消息并尝试消费
3. 在运行过程中停止 RabbitMQ 服务：brew services stop rabbitmq
4. 观察脚本如何处理连接失败并自动重试
5. 重新启动 RabbitMQ 服务：brew services start rabbitmq
6. 观察脚本如何自动恢复连接并继续工作

预期行为：
- 连接失败时会自动重试
- 发送失败时会自动重试
- 服务恢复后会立即重新连接
- 整个过程中程序不会崩溃退出
"""

import logging
import time
from datetime import datetime
import signal
import sys

from use_rabbitmq import RabbitMQStore

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SimpleResilienceTest:
    def __init__(self):
        self.running = True
        self.rabbitmq_store = None
        self.message_count = 0
        self.success_count = 0
        self.failure_count = 0
        
    def setup_signal_handler(self):
        """设置信号处理器"""
        def signal_handler(signum, frame):
            logger.info("🛑 收到退出信号，正在停止...")
            self.running = False
            
        signal.signal(signal.SIGINT, signal_handler)
        
    def get_rabbitmq_connection(self):
        """获取RabbitMQ连接，失败时返回None"""
        try:
            if not self.rabbitmq_store:
                logger.info("🔄 尝试连接到 RabbitMQ...")
                self.rabbitmq_store = RabbitMQStore(
                    host='localhost',
                    port=5672,
                    username='admin',
                    password='admin'
                )
                
                # 测试连接
                _ = self.rabbitmq_store.connection
                logger.info("✅ RabbitMQ 连接成功!")
                
            return self.rabbitmq_store
            
        except Exception as e:
            logger.error(f"❌ RabbitMQ 连接失败: {e}")
            self.rabbitmq_store = None
            return None
            
    def test_send_messages(self):
        """测试发送消息的弹性"""
        queue_name = "resilience_test_simple"
        
        while self.running:
            try:
                # 获取连接
                store = self.get_rabbitmq_connection()
                if not store:
                    logger.warning("⏳ 连接不可用，3秒后重试...")
                    time.sleep(3)
                    continue
                
                # 声明队列
                store.declare_queue(queue_name, durable=True)
                
                # 发送消息
                self.message_count += 1
                message = f"弹性测试消息 #{self.message_count} - {datetime.now().strftime('%H:%M:%S')}"
                
                logger.info(f"📤 发送消息: {message}")
                result = store.send(queue_name, message)
                
                self.success_count += 1
                logger.info(f"✅ 发送成功! (成功: {self.success_count}, 失败: {self.failure_count})")
                
                # 检查队列中的消息数量
                try:
                    count = store.get_message_counts(queue_name)
                    logger.info(f"📊 队列中消息数量: {count}")
                except Exception as e:
                    logger.warning(f"⚠️ 获取消息数量失败: {e}")
                
                time.sleep(2)  # 每2秒发送一条消息
                
            except Exception as e:
                self.failure_count += 1
                logger.error(f"❌ 发送失败: {e} (成功: {self.success_count}, 失败: {self.failure_count})")
                
                # 重置连接，强制重新连接
                self.rabbitmq_store = None
                time.sleep(3)
                
    def test_consume_messages(self):
        """测试消费消息的弹性"""
        queue_name = "resilience_test_simple"
        
        def message_handler(message):
            try:
                content = message.body.decode('utf-8')
                logger.info(f"📥 收到消息: {content}")
                time.sleep(0.1)  # 模拟处理时间
            except Exception as e:
                logger.error(f"❌ 消息处理失败: {e}")
                raise
                
        while self.running:
            try:
                # 获取连接
                store = self.get_rabbitmq_connection()
                if not store:
                    logger.warning("⏳ 连接不可用，等待重试...")
                    time.sleep(3)
                    continue
                    
                # 声明队列
                store.declare_queue(queue_name, durable=True)
                
                logger.info("📥 开始消费消息...")
                # 开始消费（这会阻塞直到出错）
                store.start_consuming(
                    queue_name=queue_name,
                    callback=message_handler,
                    prefetch=1
                )
                
            except Exception as e:
                logger.error(f"❌ 消费失败: {e}")
                self.rabbitmq_store = None
                if self.running:
                    logger.info("🔄 5秒后重试消费...")
                    time.sleep(5)
                    
    def run_producer_test(self):
        """运行生产者测试"""
        logger.info("🚀 启动生产者弹性测试")
        logger.info("💡 提示：在运行过程中可以停止/重启 RabbitMQ 来测试弹性")
        logger.info("💡 停止命令：brew services stop rabbitmq")
        logger.info("💡 启动命令：brew services start rabbitmq")
        logger.info("💡 停止测试：按 Ctrl+C")
        print()
        
        self.setup_signal_handler()
        
        try:
            self.test_send_messages()
        except KeyboardInterrupt:
            pass
        finally:
            self.cleanup()
            
    def run_consumer_test(self):
        """运行消费者测试"""
        logger.info("🚀 启动消费者弹性测试")
        logger.info("💡 提示：先运行生产者测试产生消息，然后运行此消费者测试")
        logger.info("💡 在消费过程中可以停止/重启 RabbitMQ 来测试弹性")
        print()
        
        self.setup_signal_handler()
        
        try:
            self.test_consume_messages()
        except KeyboardInterrupt:
            pass
        finally:
            self.cleanup()
            
    def run_full_test(self):
        """运行完整测试（生产者+消费者）"""
        import threading
        
        logger.info("🚀 启动完整弹性测试（生产者+消费者）")
        logger.info("💡 提示：在运行过程中可以停止/重启 RabbitMQ 来测试弹性")
        print()
        
        self.setup_signal_handler()
        
        # 启动消费者线程
        consumer_thread = threading.Thread(target=self.test_consume_messages, daemon=True)
        consumer_thread.start()
        
        # 等待一下让消费者先启动
        time.sleep(2)
        
        try:
            # 在主线程运行生产者
            self.test_send_messages()
        except KeyboardInterrupt:
            pass
        finally:
            self.cleanup()
            
    def cleanup(self):
        """清理资源"""
        logger.info("🧹 清理资源...")
        self.running = False
        
        if self.rabbitmq_store:
            try:
                self.rabbitmq_store.shutdown()
                logger.info("✅ RabbitMQ 连接已关闭")
            except Exception as e:
                logger.error(f"❌ 关闭连接时出错: {e}")
                
        logger.info(f"📊 最终统计 - 成功: {self.success_count}, 失败: {self.failure_count}")
        logger.info("🏁 测试结束")

def main():
    """主函数"""
    print("=" * 50)
    print("🐰 RabbitMQ 简化弹性测试")
    print("=" * 50)
    print()
    print("选择测试模式:")
    print("1. 生产者测试 (只发送消息)")
    print("2. 消费者测试 (只消费消息)")
    print("3. 完整测试 (生产者+消费者)")
    print()
    
    try:
        choice = input("请选择 (1/2/3): ").strip()
        
        test = SimpleResilienceTest()
        
        if choice == "1":
            test.run_producer_test()
        elif choice == "2":
            test.run_consumer_test()
        elif choice == "3":
            test.run_full_test()
        else:
            print("❌ 无效选择")
            return
            
    except KeyboardInterrupt:
        print("\n测试已取消")
    except Exception as e:
        print(f"❌ 测试出错: {e}")

if __name__ == "__main__":
    main()