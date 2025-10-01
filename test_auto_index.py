#!/usr/bin/env python3
"""
测试自动获取账户index功能
"""

import asyncio
import sys
import os

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.config.config_manager import ConfigManager
from src.services.account_manager import AccountManager
import structlog

# 配置日志
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
        structlog.processors.JSONRenderer()
    ],
    wrapper_class=structlog.stdlib.BoundLogger,
    logger_factory=structlog.stdlib.LoggerFactory(),
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger()


async def test_auto_index_lookup():
    """测试自动获取账户index功能"""
    
    print("🧪 测试自动获取账户index功能")
    print("=" * 60)
    
    try:
        # 初始化配置管理器
        config_manager = ConfigManager()
        await config_manager.initialize()
        
        # 初始化账户管理器
        account_manager = AccountManager(config_manager)
        await account_manager.initialize()
        
        print("✅ 系统初始化成功")
        
        # 获取配置中的账户
        accounts = config_manager.get_accounts()
        
        print(f"\n📋 发现 {len(accounts)} 个配置的账户:")
        
        for account_config in accounts:
            print(f"\n账户: {account_config.l1_address}")
            print(f"  配置的index: {account_config.index}")
            print(f"  是否活跃: {account_config.is_active}")
            
            # 如果index为0，测试自动获取功能
            if account_config.index == 0:
                print(f"  🔍 检测到index为0，测试自动获取功能...")
                
                # 直接调用获取index的方法进行测试
                detected_index = await account_manager._get_account_index_by_address(
                    account_config.l1_address
                )
                
                if detected_index != 0:
                    print(f"  ✅ 成功检测到正确的index: {detected_index}")
                else:
                    print(f"  ❌ 无法检测到有效的index")
            else:
                print(f"  ℹ️  index已配置，无需自动获取")
                
                # 验证配置的index是否正确
                try:
                    account_data = await account_manager.get_account(account_config.index)
                    if account_data:
                        print(f"  ✅ 配置的index {account_config.index} 有效")
                    else:
                        print(f"  ⚠️  配置的index {account_config.index} 可能无效")
                except Exception as e:
                    print(f"  ❌ 验证index时出错: {e}")
        
        print(f"\n💡 使用说明:")
        print(f"1. 如果您的配置文件中账户index设置为0:")
        print(f"   系统将自动通过钱包地址查找正确的index")
        print(f"2. 自动获取的index会更新到内存配置中")
        print(f"3. 可选择是否保存更新后的index到配置文件")
        
        print(f"\n🔧 配置示例:")
        print(f"accounts:")
        print(f"  - index: 0  # 设置为0以启用自动获取")
        print(f"    l1_address: \"0xYourWalletAddress\"")
        print(f"    private_key: \"your_private_key\"")
        print(f"    api_key_index: 111")
        
    except Exception as e:
        logger.error("测试失败", error=str(e))
        print(f"❌ 测试失败: {e}")
        return False
    
    print(f"\n✅ 测试完成")
    return True


async def test_get_account_by_address(l1_address: str):
    """测试通过地址获取账户信息"""
    
    print(f"\n🔍 测试通过地址获取账户信息")
    print(f"地址: {l1_address}")
    print("-" * 40)
    
    try:
        # 初始化配置管理器
        config_manager = ConfigManager()
        await config_manager.initialize()
        
        # 初始化账户管理器
        account_manager = AccountManager(config_manager)
        await account_manager.initialize()
        
        # 测试获取账户index
        account_index = await account_manager._get_account_index_by_address(l1_address)
        
        if account_index != 0:
            print(f"✅ 成功获取账户index: {account_index}")
            
            # 尝试获取账户详细信息
            try:
                account_data = await account_manager.get_account(account_index)
                if account_data:
                    print(f"账户余额: ${float(account_data.balance):.2f}")
                    print(f"可用余额: ${float(account_data.available_balance):.2f}")
                    print(f"仓位数量: {len(account_data.positions)}")
                else:
                    print("无法获取账户详细信息")
            except Exception as e:
                print(f"获取账户详细信息时出错: {e}")
        else:
            print(f"❌ 无法获取有效的账户index")
            
    except Exception as e:
        print(f"❌ 测试失败: {e}")


async def main():
    """主函数"""
    import sys
    
    if len(sys.argv) > 1:
        # 如果提供了地址参数，测试特定地址
        l1_address = sys.argv[1]
        await test_get_account_by_address(l1_address)
    else:
        # 否则运行完整测试
        await test_auto_index_lookup()


if __name__ == "__main__":
    print("🔧 账户Index自动获取测试工具")
    print("用法:")
    print("  python test_auto_index.py                      # 测试配置中的所有账户")
    print("  python test_auto_index.py 0xYourWalletAddress  # 测试特定钱包地址")
    print()
    
    asyncio.run(main())