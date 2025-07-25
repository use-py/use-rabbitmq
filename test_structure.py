#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
结构测试脚本，验证 RabbitConnectionFactory 的代码结构是否正确
不需要实际连接到RabbitMQ服务
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

def test_imports():
    """测试导入"""
    print("=== 测试导入 ===")
    
    try:
        from use_rabbitmq import (
            RabbitConnectionFactory,
            rabbitConnectionFactory,
            get_default_factory,
            create_rabbit_connection,
            get_connection_manager,
            ConnectionManager
        )
        print("✅ 所有类和函数导入成功")
        return True
    except ImportError as e:
        print(f"❌ 导入失败: {e}")
        return False

def test_factory_creation():
    """测试工厂创建"""
    print("\n=== 测试工厂创建 ===")
    
    try:
        from use_rabbitmq import RabbitConnectionFactory
        
        # 测试默认创建
        factory1 = RabbitConnectionFactory()
        print("✅ 默认工厂创建成功")
        
        # 测试带client_name创建
        factory2 = RabbitConnectionFactory(client_name="TestApp")
        print("✅ 带client_name的工厂创建成功")
        
        # 测试带配置创建
        config = {
            "host": "localhost",
            "port": 5672,
            "username": "guest",
            "password": "guest"
        }
        factory3 = RabbitConnectionFactory(client_name="ConfigApp", default_config=config)
        print("✅ 带配置的工厂创建成功")
        
        return True
    except Exception as e:
        print(f"❌ 工厂创建失败: {e}")
        return False

def test_factory_methods():
    """测试工厂方法"""
    print("\n=== 测试工厂方法 ===")
    
    try:
        from use_rabbitmq import RabbitConnectionFactory
        
        factory = RabbitConnectionFactory(client_name="MethodTest")
        
        # 测试方法存在性
        methods = [
            'get_connection_manager',
            'create_connection',
            'remove_connection_manager',
            'list_connections',
            'shutdown_all'
        ]
        
        for method in methods:
            if hasattr(factory, method):
                print(f"✅ 方法 {method} 存在")
            else:
                print(f"❌ 方法 {method} 不存在")
                return False
        
        # 测试上下文管理器方法
        if hasattr(factory, '__enter__') and hasattr(factory, '__exit__'):
            print("✅ 上下文管理器方法存在")
        else:
            print("❌ 上下文管理器方法不存在")
            return False
        
        return True
    except Exception as e:
        print(f"❌ 方法测试失败: {e}")
        return False

def test_connection_manager_structure():
    """测试连接管理器结构"""
    print("\n=== 测试连接管理器结构 ===")
    
    try:
        from use_rabbitmq import ConnectionManager
        
        # 测试ConnectionManager类存在
        print("✅ ConnectionManager类存在")
        
        # 测试方法存在性
        methods = [
            'create_channel',
            'get_channel',
            'close_channel',
            'list_channels',
            'shutdown'
        ]
        
        for method in methods:
            if hasattr(ConnectionManager, method):
                print(f"✅ ConnectionManager方法 {method} 存在")
            else:
                print(f"❌ ConnectionManager方法 {method} 不存在")
                return False
        
        return True
    except Exception as e:
        print(f"❌ ConnectionManager结构测试失败: {e}")
        return False

def test_convenience_functions():
    """测试便捷函数"""
    print("\n=== 测试便捷函数 ===")
    
    try:
        from use_rabbitmq import (
            get_default_factory,
            create_rabbit_connection,
            get_connection_manager,
            rabbitConnectionFactory
        )
        
        # 测试函数存在
        functions = [
            ('get_default_factory', get_default_factory),
            ('create_rabbit_connection', create_rabbit_connection),
            ('get_connection_manager', get_connection_manager),
            ('rabbitConnectionFactory', rabbitConnectionFactory)
        ]
        
        for name, func in functions:
            if callable(func):
                print(f"✅ 便捷函数 {name} 存在且可调用")
            else:
                print(f"❌ 便捷函数 {name} 不可调用")
                return False
        
        return True
    except Exception as e:
        print(f"❌ 便捷函数测试失败: {e}")
        return False

def test_client_name_feature():
    """测试client_name功能"""
    print("\n=== 测试client_name功能 ===")
    
    try:
        from use_rabbitmq import RabbitConnectionFactory
        
        # 测试client_name设置
        factory = RabbitConnectionFactory(client_name="TestClientName")
        
        # 检查内部属性
        if hasattr(factory, '_client_name'):
            client_name = factory._client_name
            if client_name == "TestClientName":
                print("✅ client_name设置正确")
            else:
                print(f"❌ client_name设置错误，期望: TestClientName, 实际: {client_name}")
                return False
        else:
            print("❌ _client_name属性不存在")
            return False
        
        # 测试默认client_name
        factory2 = RabbitConnectionFactory()
        if hasattr(factory2, '_client_name'):
            default_name = factory2._client_name
            if default_name.startswith("rabbitConnectionFactory#"):
                print("✅ 默认client_name格式正确")
            else:
                print(f"❌ 默认client_name格式错误: {default_name}")
                return False
        
        return True
    except Exception as e:
        print(f"❌ client_name功能测试失败: {e}")
        return False

def test_thread_safety_structure():
    """测试线程安全结构"""
    print("\n=== 测试线程安全结构 ===")
    
    try:
        from use_rabbitmq import RabbitConnectionFactory
        import threading
        
        factory = RabbitConnectionFactory(client_name="ThreadTest")
        
        # 检查锁存在
        if hasattr(factory, '_lock'):
            lock = factory._lock
            if hasattr(lock, 'acquire') and hasattr(lock, 'release'):
                print("✅ 工厂线程锁存在")
            else:
                print("❌ 工厂线程锁类型错误")
                return False
        else:
            print("❌ 工厂线程锁不存在")
            return False
        
        return True
    except Exception as e:
        print(f"❌ 线程安全结构测试失败: {e}")
        return False

if __name__ == "__main__":
    print("RabbitConnectionFactory 结构测试")
    print("=" * 50)
    print("此测试验证代码结构，不需要RabbitMQ服务运行\n")
    
    tests = [
        test_imports,
        test_factory_creation,
        test_factory_methods,
        test_connection_manager_structure,
        test_convenience_functions,
        test_client_name_feature,
        test_thread_safety_structure
    ]
    
    passed = 0
    total = len(tests)
    
    for test in tests:
        if test():
            passed += 1
    
    print(f"\n测试结果: {passed}/{total} 通过")
    
    if passed == total:
        print("🎉 所有结构测试通过！")
        print("\n实现的功能:")
        print("✅ RabbitConnectionFactory 类")
        print("✅ ConnectionManager 类")
        print("✅ Client-provided name 支持")
        print("✅ 多channel管理结构")
        print("✅ 线程安全设计")
        print("✅ 上下文管理器支持")
        print("✅ 便捷函数接口")
        print("\n代码结构完整，可以开始使用！")
    else:
        print("⚠️  部分结构测试失败，请检查代码实现")
        sys.exit(1)