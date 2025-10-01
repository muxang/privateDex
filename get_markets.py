#!/usr/bin/env python3
"""
获取Lighter协议支持的所有市场及其market_index
"""

import asyncio
import sys
import os

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.config.config_manager import ConfigManager
from src.core.lighter_client_factory import LighterClientFactory
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


async def get_all_markets():
    """获取所有可用的市场信息"""
    
    config_manager = ConfigManager()
    await config_manager.initialize()
    
    client_factory = LighterClientFactory(config_manager)
    
    try:
        print("🔍 正在获取Lighter协议支持的所有市场...")
        print("=" * 60)
        
        # 获取Info API客户端
        info_api = client_factory.get_info_api()
        
        try:
            # 获取所有市场信息
            markets_response = await info_api.get_all_markets()
            
            if hasattr(markets_response, 'markets') and markets_response.markets:
                markets = markets_response.markets
                print(f"📊 发现 {len(markets)} 个市场:")
                print()
                
                # 按market_index排序
                sorted_markets = sorted(markets, key=lambda m: m.market_index if hasattr(m, 'market_index') else 0)
                
                for market in sorted_markets:
                    market_index = getattr(market, 'market_index', 'N/A')
                    symbol = getattr(market, 'symbol', 'N/A')
                    base_asset = getattr(market, 'base_asset', 'N/A')
                    quote_asset = getattr(market, 'quote_asset', 'N/A')
                    is_active = getattr(market, 'is_active', False)
                    min_size = getattr(market, 'min_size', 'N/A')
                    tick_size = getattr(market, 'tick_size', 'N/A')
                    
                    status = "✅ 活跃" if is_active else "❌ 不活跃"
                    
                    print(f"Market Index: {market_index}")
                    print(f"  交易对: {symbol}")
                    print(f"  基础资产: {base_asset}")
                    print(f"  计价资产: {quote_asset}")
                    print(f"  状态: {status}")
                    print(f"  最小下单量: {min_size}")
                    print(f"  价格精度: {tick_size}")
                    print("-" * 40)
                
                print(f"\n💡 配置示例:")
                print("# 在config.yaml中使用以下market_index:")
                active_markets = [m for m in sorted_markets if getattr(m, 'is_active', False)]
                for market in active_markets[:5]:  # 显示前5个活跃市场
                    market_index = getattr(market, 'market_index', 'N/A')
                    symbol = getattr(market, 'symbol', 'N/A')
                    print(f"# {symbol}: market_index: {market_index}")
                    
            else:
                print("❌ 未获取到市场数据")
                
        except Exception as api_error:
            logger.error("API调用失败", error=str(api_error))
            print(f"❌ API调用失败: {api_error}")
            
            # 尝试其他API方法
            print("\n🔄 尝试获取单个市场信息...")
            try:
                # 尝试获取常见的市场索引
                common_markets = [1, 2, 3, 4, 5]  # BTC, ETH, SOL等常见市场
                found_markets = []
                
                for market_idx in common_markets:
                    try:
                        market_info = await info_api.get_market_info(market_index=market_idx)
                        if market_info:
                            found_markets.append((market_idx, market_info))
                            print(f"✅ Market {market_idx}: 有效")
                        else:
                            print(f"❌ Market {market_idx}: 无效")
                    except:
                        print(f"❌ Market {market_idx}: 无法访问")
                
                if found_markets:
                    print(f"\n📋 发现的有效市场:")
                    for market_idx, market_info in found_markets:
                        print(f"  - Market Index {market_idx}: {market_info}")
                        
            except Exception as fallback_error:
                logger.error("备用方法也失败", error=str(fallback_error))
                print(f"❌ 备用方法失败: {fallback_error}")
    
    except Exception as e:
        logger.error("获取市场信息失败", error=str(e))
        print(f"❌ 错误: {e}")
        
    finally:
        # 清理客户端
        await client_factory.cleanup()


async def get_market_by_symbol(symbol: str):
    """根据交易对符号获取market_index"""
    
    config_manager = ConfigManager()
    await config_manager.initialize()
    
    client_factory = LighterClientFactory(config_manager)
    
    try:
        print(f"🔍 正在查找交易对 {symbol} 的market_index...")
        
        info_api = client_factory.get_info_api()
        
        # 获取所有市场
        markets_response = await info_api.get_all_markets()
        
        if hasattr(markets_response, 'markets') and markets_response.markets:
            for market in markets_response.markets:
                market_symbol = getattr(market, 'symbol', '')
                if market_symbol.upper() == symbol.upper():
                    market_index = getattr(market, 'market_index', 'N/A')
                    is_active = getattr(market, 'is_active', False)
                    print(f"✅ 找到匹配: {symbol}")
                    print(f"   Market Index: {market_index}")
                    print(f"   状态: {'活跃' if is_active else '不活跃'}")
                    return market_index
            
            print(f"❌ 未找到交易对 {symbol}")
            print("支持的交易对:")
            for market in markets_response.markets:
                symbol = getattr(market, 'symbol', 'N/A')
                market_index = getattr(market, 'market_index', 'N/A')
                print(f"  - {symbol} (Index: {market_index})")
        else:
            print("❌ 无法获取市场列表")
            
    except Exception as e:
        logger.error("查找市场失败", error=str(e))
        print(f"❌ 错误: {e}")
        
    finally:
        await client_factory.cleanup()


def print_help():
    """打印帮助信息"""
    print("🔧 Lighter市场查询工具")
    print("=" * 40)
    print("用法:")
    print("  python get_markets.py                # 获取所有市场")
    print("  python get_markets.py BTC-USD        # 查找特定交易对")
    print("  python get_markets.py --help         # 显示帮助")
    print()
    print("示例:")
    print("  python get_markets.py BTC-USD")
    print("  python get_markets.py ETH-USD")
    print("  python get_markets.py SOL-USD")


async def main():
    """主函数"""
    import sys
    
    if len(sys.argv) == 1:
        # 获取所有市场
        await get_all_markets()
    elif len(sys.argv) == 2:
        arg = sys.argv[1]
        if arg in ['--help', '-h', 'help']:
            print_help()
        else:
            # 查找特定交易对
            await get_market_by_symbol(arg)
    else:
        print("❌ 参数错误")
        print_help()


if __name__ == "__main__":
    asyncio.run(main())