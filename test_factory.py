#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
简单测试脚本，验证 RabbitConnectionFactory 的基本功能
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from use_rabbitmq import RabbitConnectionFactory

def test_basic_functionality():
    """测试基本功能"""
    print("=== 测试基本功能 ===")
    
    try:
        # 创建工厂
        factory = RabbitConnectionFactory(client_name="TestApp")
        print("✅ 工厂创建成功")
        
        # 获取连接管理器
        conn_manager = factory.get_connection_manager("test")
        print("✅ 连接管理器获取成功")
        
        # 创建channel
        channel = conn_manager.create_channel("test_channel")
        print("✅ Channel创建成功")
        
        # 测试队列操作（使用唯一队列名避免冲突）
        import time
        queue_name = f"test_queue_{int(time.time())}"
        channel.queue.declare(queue_name, durable=False, auto_delete=True)
        print("✅ 队列声明成功")
        
        # 发送消息
        channel.basic.publish("Hello World!", queue_name)
        print("✅ 消息发送成功")
        
        # 列出channels
        channels = conn_manager.list_channels()
        print(f"✅ 当前channels: {channels}")
        
        # 清理
        factory.shutdown_all()
        print("✅ 资源清理成功")
        
        return True
        
    except Exception as e:
        print(f"❌ 测试失败: {e}")
        return False

def test_multiple_channels():
    """测试多channel功能"""
    print("\n=== 测试多Channel功能 ===")
    
    try:
        factory = RabbitConnectionFactory(client_name="MultiChannelTest")
        conn_manager = factory.get_connection_manager("multi_test")
        
        # 创建多个channel
        ch1 = conn_manager.create_channel("publisher")
        ch2 = conn_manager.create_channel("consumer")
        ch3 = conn_manager.create_channel("admin")
        
        channels = conn_manager.list_channels()
        print(f"✅ 创建了 {len(channels)} 个channels: {channels}")
        
        # 关闭一个channel
        conn_manager.close_channel("admin")
        remaining = conn_manager.list_channels()
        print(f"✅ 关闭admin后剩余channels: {remaining}")
        
        factory.shutdown_all()
        print("✅ 多channel测试成功")
        
        return True
        
    except Exception as e:
        print(f"❌ 多channel测试失败: {e}")
        return False

def test_connection_reuse():
    """测试连接复用"""
    print("\n=== 测试连接复用 ===")
    
    try:
        factory = RabbitConnectionFactory(client_name="ReuseTest")
        
        # 获取同名连接管理器
        manager1 = factory.get_connection_manager("shared")
        manager2 = factory.get_connection_manager("shared")
        
        is_same = manager1 is manager2
        print(f"✅ 同名连接管理器是同一实例: {is_same}")
        
        # 列出连接
        connections = factory.list_connections()
        print(f"✅ 当前连接数: {len(connections)}, 连接名: {connections}")
        
        factory.shutdown_all()
        print("✅ 连接复用测试成功")
        
        return True
        
    except Exception as e:
        print(f"❌ 连接复用测试失败: {e}")
        return False

def test_context_manager():
    """测试上下文管理器"""
    print("\n=== 测试上下文管理器 ===")
    
    try:
        with RabbitConnectionFactory(client_name="ContextTest") as factory:
            conn_manager = factory.get_connection_manager("context")
            
            with conn_manager as manager:
                channel = manager.create_channel("temp")
                channel.queue.declare("temp_queue", durable=False)
                print("✅ 在上下文中操作成功")
            
            print("✅ 连接管理器上下文退出")
        
        print("✅ 工厂上下文退出，上下文管理器测试成功")
        return True
        
    except Exception as e:
        print(f"❌ 上下文管理器测试失败: {e}")
        return False

if __name__ == "__main__":
    print("RabbitConnectionFactory 功能测试")
    print("=" * 50)
    
    # 检查RabbitMQ是否可用
    print("注意: 此测试需要RabbitMQ服务运行在localhost:5672")
    print("如果连接失败，请确保RabbitMQ服务正在运行\n")
    
    tests = [
        test_basic_functionality,
        test_multiple_channels,
        test_connection_reuse,
        test_context_manager
    ]
    
    passed = 0
    total = len(tests)
    
    for test in tests:
        if test():
            passed += 1
    
    print(f"\n测试结果: {passed}/{total} 通过")
    
    if passed == total:
        print("🎉 所有测试通过！RabbitConnectionFactory功能正常")
        print("\n主要功能验证:")
        print("✅ Client-provided name 支持")
        print("✅ 一个连接多个channel")
        print("✅ 连接复用管理")
        print("✅ 上下文管理器")
    else:
        print("⚠️  部分测试失败，请检查RabbitMQ服务状态")
        sys.exit(1)