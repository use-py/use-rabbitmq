#!/usr/bin/env python3
"""
API演示示例

这个示例展示了RabbitMQ库增强功能的API使用方法，
不需要实际的RabbitMQ服务器连接。
"""

import logging
from use_rabbitmq import RabbitMQStore, ConnectionPool, ChannelManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def demo_enhanced_initialization():
    """演示增强功能的初始化"""
    print("\n=== 增强功能初始化演示 ===")
    
    # 1. 基本使用（向后兼容）
    print("1. 基本使用（向后兼容）:")
    basic_mq = RabbitMQStore(
        host="localhost",
        port=5672,
        username="guest",
        password="guest"
    )
    print(f"   客户端名称: {basic_mq.client_name}")
    print(f"   使用连接池: {basic_mq.use_connection_pool}")
    print(f"   使用通道管理器: {basic_mq.use_channel_manager}")
    
    # 2. 自定义客户端名称
    print("\n2. 自定义客户端名称:")
    named_mq = RabbitMQStore(
        host="localhost",
        client_name="my-producer-service"
    )
    print(f"   客户端名称: {named_mq.client_name}")
    
    # 3. 禁用增强功能（传统模式）
    print("\n3. 传统模式（禁用增强功能）:")
    traditional_mq = RabbitMQStore(
        host="localhost",
        client_name="traditional-client",
        use_connection_pool=False,
        use_channel_manager=False
    )
    print(f"   使用连接池: {traditional_mq.use_connection_pool}")
    print(f"   使用通道管理器: {traditional_mq.use_channel_manager}")
    
    # 4. 完全自定义配置
    print("\n4. 完全自定义配置:")
    custom_mq = RabbitMQStore(
        host="localhost",
        port=5672,
        username="admin",
        password="admin123",
        client_name="high-performance-client",
        use_connection_pool=True,
        use_channel_manager=True,
        max_channels=50,
        confirm_delivery=True
    )
    print(f"   客户端名称: {custom_mq.client_name}")
    print(f"   最大通道数: {custom_mq.max_channels}")
    print(f"   消息确认: {custom_mq.confirm_delivery}")

def demo_client_info():
    """演示客户端信息获取"""
    print("\n=== 客户端信息演示 ===")
    
    mq = RabbitMQStore(
        host="rabbitmq.example.com",
        port=5672,
        username="myuser",
        password="mypass",
        client_name="info-demo-client",
        use_connection_pool=True,
        use_channel_manager=True,
        max_channels=15
    )
    
    # 获取客户端信息
    info = mq.get_client_info()
    
    print("客户端完整信息:")
    print(f"  客户端名称: {info['client_name']}")
    print(f"  连接池启用: {info['use_connection_pool']}")
    print(f"  通道管理器启用: {info['use_channel_manager']}")
    print(f"  最大通道数: {info['max_channels']}")
    print(f"  消息确认: {info['confirm_delivery']}")
    
    print("\n连接参数:")
    conn_params = info['connection_parameters']
    for key, value in conn_params.items():
        print(f"  {key}: {value}")
    
    print("\n统计信息:")
    print(f"  连接池统计: {info['connection_pool_stats']}")
    print(f"  通道管理器统计: {info['channel_manager_stats']}")

def demo_connection_pool_api():
    """演示连接池API"""
    print("\n=== 连接池API演示 ===")
    
    # 获取连接池实例（单例模式）
    pool1 = ConnectionPool()
    pool2 = ConnectionPool()
    
    print(f"连接池单例验证: {pool1 is pool2}")
    
    # 演示池键生成
    params1 = {'hostname': 'localhost', 'port': 5672, 'username': 'guest'}
    params2 = {'hostname': 'localhost', 'port': 5673, 'username': 'guest'}
    
    key1 = pool1._get_pool_key(params1)
    key2 = pool1._get_pool_key(params2)
    
    print(f"\n连接池键示例:")
    print(f"  参数1的池键: {key1}")
    print(f"  参数2的池键: {key2}")
    print(f"  键是否相同: {key1 == key2}")
    
    # 获取统计信息
    stats = pool1.get_pool_stats()
    print(f"\n连接池统计: {stats}")

def demo_usage_patterns():
    """演示使用模式"""
    print("\n=== 使用模式演示 ===")
    
    # 1. 上下文管理器模式（推荐）
    print("1. 上下文管理器模式:")
    print("   with RabbitMQStore(...) as mq:")
    print("       mq.send('queue', 'message')")
    print("   # 自动清理资源")
    
    # 2. 多通道模式
    print("\n2. 多通道模式:")
    print("   with mq.get_channel() as channel:")
    print("       channel.basic.publish(message, queue)")
    print("   # 通道自动返回到池中")
    
    # 3. 传统模式（向后兼容）
    print("\n3. 传统模式:")
    print("   mq.send('queue', 'message')")
    print("   # 使用单一通道")
    
    # 4. 监控模式
    print("\n4. 监控模式:")
    print("   info = mq.get_client_info()")
    print("   pool_stats = mq.get_connection_pool_stats()")
    print("   channel_stats = mq.get_channel_manager_stats()")

def demo_configuration_examples():
    """演示配置示例"""
    print("\n=== 配置示例演示 ===")
    
    configs = [
        {
            'name': '高性能生产者',
            'config': {
                'client_name': 'high-perf-producer',
                'use_connection_pool': True,
                'use_channel_manager': True,
                'max_channels': 50,
                'confirm_delivery': True
            }
        },
        {
            'name': '简单消费者',
            'config': {
                'client_name': 'simple-consumer',
                'use_connection_pool': True,
                'use_channel_manager': False,
                'confirm_delivery': False
            }
        },
        {
            'name': '传统兼容模式',
            'config': {
                'client_name': 'legacy-client',
                'use_connection_pool': False,
                'use_channel_manager': False
            }
        },
        {
            'name': '微服务模式',
            'config': {
                'client_name': 'microservice-api',
                'use_connection_pool': True,
                'use_channel_manager': True,
                'max_channels': 20,
                'confirm_delivery': True
            }
        }
    ]
    
    for config_info in configs:
        print(f"\n{config_info['name']}:")
        config = config_info['config']
        for key, value in config.items():
            print(f"  {key}: {value}")
        
        # 创建实例（仅演示，不实际连接）
        mq = RabbitMQStore(host="localhost", **config)
        print(f"  实际客户端名称: {mq.client_name}")

def demo_migration_guide():
    """演示迁移指南"""
    print("\n=== 迁移指南演示 ===")
    
    print("从旧版本迁移到新版本:")
    print("\n1. 无需修改现有代码（完全向后兼容）:")
    print("   # 旧代码")
    print("   mq = RabbitMQStore(host='localhost', username='guest', password='guest')")
    print("   # 新版本中仍然正常工作，自动启用增强功能")
    
    print("\n2. 逐步启用新功能:")
    print("   # 添加客户端名称")
    print("   mq = RabbitMQStore(..., client_name='my-service')")
    print("   ")
    print("   # 使用多通道模式")
    print("   with mq.get_channel() as channel:")
    print("       channel.basic.publish(message, queue)")
    
    print("\n3. 监控和调试:")
    print("   # 获取客户端信息")
    print("   info = mq.get_client_info()")
    print("   print(f'客户端: {info[\"client_name\"]}')")
    print("   ")
    print("   # 获取性能统计")
    print("   pool_stats = mq.get_connection_pool_stats()")
    print("   channel_stats = mq.get_channel_manager_stats()")
    
    print("\n4. 性能优化:")
    print("   # 高并发场景")
    print("   mq = RabbitMQStore(..., max_channels=50)")
    print("   ")
    print("   # 禁用不需要的功能")
    print("   mq = RabbitMQStore(..., use_channel_manager=False)")

if __name__ == "__main__":
    print("RabbitMQ 增强功能 API 演示")
    print("=" * 60)
    
    demo_enhanced_initialization()
    demo_client_info()
    demo_connection_pool_api()
    demo_usage_patterns()
    demo_configuration_examples()
    demo_migration_guide()
    
    print("\n" + "=" * 60)
    print("API演示完成！")
    print("\n核心增强功能总结:")
    print("1. 连接池管理 - 自动复用连接，提高性能")
    print("2. 客户端自定义名称 - 便于监控和调试")
    print("3. 单连接多通道 - 支持高并发操作")
    print("4. 完全向后兼容 - 无需修改现有代码")
    print("5. 丰富的监控API - 实时了解连接状态")