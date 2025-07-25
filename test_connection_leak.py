#!/usr/bin/env python3
"""
连接泄漏测试脚本

此脚本用于验证RabbitMQ连接泄漏问题是否已修复。
运行此脚本时，请同时监控RabbitMQ管理界面中的连接数。
"""

import time
import threading
from use_rabbitmq import RabbitMQStore

def test_connection_cleanup():
    """测试连接清理"""
    print("开始测试连接清理...")
    
    for i in range(10):
        print(f"创建第 {i+1} 个RabbitMQ实例")
        
        # 创建RabbitMQ实例
        mq = RabbitMQStore(
            host="localhost",
            port=5672,
            username="guest",
            password="guest"
        )
        
        try:
            # 访问连接和通道以确保它们被创建
            _ = mq.connection
            _ = mq.channel
            print(f"  - 连接和通道已创建")
            
            # 模拟一些操作
            time.sleep(0.1)
            
        except Exception as e:
            print(f"  - 连接失败: {e}")
        finally:
            # 显式关闭连接
            mq.shutdown()
            print(f"  - 连接已关闭")
            
        # 等待一段时间让连接完全关闭
        time.sleep(0.5)
    
    print("连接清理测试完成")

def test_multiple_instances():
    """测试多个实例同时存在"""
    print("\n开始测试多个实例...")
    
    instances = []
    
    # 创建多个实例
    for i in range(5):
        try:
            mq = RabbitMQStore(
                host="localhost",
                port=5672,
                username="guest",
                password="guest"
            )
            # 触发连接创建
            _ = mq.connection
            instances.append(mq)
            print(f"创建实例 {i+1}")
        except Exception as e:
            print(f"创建实例 {i+1} 失败: {e}")
    
    print(f"已创建 {len(instances)} 个实例")
    time.sleep(2)
    
    # 逐个关闭实例
    for i, mq in enumerate(instances):
        try:
            mq.shutdown()
            print(f"关闭实例 {i+1}")
            time.sleep(0.5)
        except Exception as e:
            print(f"关闭实例 {i+1} 失败: {e}")
    
    print("多实例测试完成")

def test_exception_handling():
    """测试异常情况下的连接清理"""
    print("\n开始测试异常处理...")
    
    for i in range(5):
        try:
            # 使用错误的连接参数
            mq = RabbitMQStore(
                host="nonexistent-host",
                port=9999,
                username="invalid",
                password="invalid"
            )
            
            # 尝试连接（应该失败）
            try:
                _ = mq.connection
            except Exception as e:
                print(f"预期的连接失败 {i+1}: {e}")
            
            # 即使连接失败，也要确保清理
            mq.shutdown()
            
        except Exception as e:
            print(f"异常测试 {i+1} 出错: {e}")
        
        time.sleep(0.2)
    
    print("异常处理测试完成")

def test_threading():
    """测试多线程环境下的连接管理"""
    print("\n开始测试多线程...")
    
    def worker(worker_id):
        try:
            mq = RabbitMQStore(
                host="localhost",
                port=5672,
                username="guest",
                password="guest"
            )
            
            # 访问连接
            _ = mq.connection
            _ = mq.channel
            
            print(f"线程 {worker_id} 连接成功")
            time.sleep(1)
            
            mq.shutdown()
            print(f"线程 {worker_id} 连接已关闭")
            
        except Exception as e:
            print(f"线程 {worker_id} 出错: {e}")
    
    threads = []
    for i in range(3):
        t = threading.Thread(target=worker, args=(i+1,))
        threads.append(t)
        t.start()
    
    for t in threads:
        t.join()
    
    print("多线程测试完成")

if __name__ == "__main__":
    print("RabbitMQ 连接泄漏测试")
    print("=" * 50)
    print("请在运行此脚本时监控RabbitMQ管理界面中的连接数")
    print("正常情况下，连接数应该在测试完成后回到初始值")
    print("=" * 50)
    
    try:
        test_connection_cleanup()
        test_multiple_instances()
        test_exception_handling()
        test_threading()
        
        print("\n" + "=" * 50)
        print("所有测试完成！")
        print("请检查RabbitMQ管理界面，确认连接数已恢复正常")
        
    except KeyboardInterrupt:
        print("\n测试被用户中断")
    except Exception as e:
        print(f"\n测试过程中出现错误: {e}")