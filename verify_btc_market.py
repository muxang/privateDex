#!/usr/bin/env python3
"""
验证BTC市场配置脚本
确认market_index对应的交易对信息
"""

import asyncio
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

async def verify_btc_market():
    """验证BTC市场配置"""
    print("=== 验证BTC市场配置 ===")
    
    try:
        from config.config_manager import ConfigManager
        from core.lighter_client_factory import LighterClientFactory
        
        # 初始化配置
        config_manager = ConfigManager()
        await config_manager.initialize()
        
        # 获取交易对配置
        trading_pairs = config_manager.get_trading_pairs()
        
        print(f"\n[配置文件中的交易对]")
        for pair in trading_pairs:
            print(f"ID: {pair.id}")
            print(f"名称: {pair.name}")
            print(f"市场索引: {pair.market_index}")
            print(f"基础数量: {pair.base_amount}")
            print(f"是否启用: {pair.is_enabled}")
            print("-" * 40)
        
        # 使用API获取市场信息
        print(f"\n[API市场信息验证]")
        factory = LighterClientFactory(config_manager)
        
        try:
            # 获取信息API
            info_api = factory.get_info_api()
            
            print("正在获取市场信息...")
            
            # 尝试获取市场信息 - 检查可用的方法
            markets_response = None
            
            # 检查InfoApi的可用方法
            available_methods = [method for method in dir(info_api) if not method.startswith('_')]
            print(f"InfoApi可用方法: {available_methods}")
            
            # 尝试不同的方法获取市场信息
            try:
                if hasattr(info_api, 'get_order_books'):
                    markets_response = await info_api.get_order_books()
                elif hasattr(info_api, 'get_order_book_details'):
                    markets_response = await info_api.get_order_book_details()
                else:
                    print("未找到合适的市场信息获取方法")
            except Exception as method_error:
                print(f"API方法调用失败: {method_error}")
            
            if hasattr(markets_response, 'markets') and markets_response.markets:
                print(f"找到 {len(markets_response.markets)} 个市场:")
                
                for i, market in enumerate(markets_response.markets):
                    print(f"\n市场索引 {i}:")
                    if hasattr(market, 'symbol'):
                        print(f"  交易对: {market.symbol}")
                    if hasattr(market, 'base_asset'):
                        print(f"  基础资产: {market.base_asset}")
                    if hasattr(market, 'quote_asset'):
                        print(f"  报价资产: {market.quote_asset}")
                    if hasattr(market, 'tick_size'):
                        print(f"  最小价格变动: {market.tick_size}")
                    if hasattr(market, 'step_size'):
                        print(f"  最小数量变动: {market.step_size}")
                    
                    # 检查是否是BTC相关
                    market_info = str(market).upper()
                    if 'BTC' in market_info or 'BITCOIN' in market_info:
                        print(f"  ✅ 这是BTC相关市场!")
                        
                        # 验证配置是否正确
                        btc_pair = next((p for p in trading_pairs if p.market_index == i), None)
                        if btc_pair:
                            print(f"  ✅ 配置文件中market_index {i}对应: {btc_pair.name}")
                        else:
                            print(f"  ❌ 配置文件中没有配置market_index {i}")
            else:
                print("无法获取市场信息，尝试其他方法...")
                
                # 尝试获取特定市场的详情
                order_api = factory.get_order_api()
                
                for market_index in [0, 1, 2]:  # 检查前几个市场
                    try:
                        print(f"\n检查市场索引 {market_index}:")
                        details_response = await order_api.order_book_details(market_id=market_index)
                        
                        if hasattr(details_response, 'order_book_details') and details_response.order_book_details:
                            detail = details_response.order_book_details[0]
                            print(f"  市场详情: {detail}")
                            
                            # 检查是否包含BTC信息
                            detail_str = str(detail).upper()
                            if 'BTC' in detail_str:
                                print(f"  ✅ 市场索引 {market_index} 是BTC市场!")
                                
                                btc_pair = next((p for p in trading_pairs if p.market_index == market_index), None)
                                if btc_pair:
                                    print(f"  ✅ 配置正确: {btc_pair.name}")
                                else:
                                    print(f"  ❌ 需要在配置中设置market_index为 {market_index}")
                        
                    except Exception as e:
                        print(f"  无法获取市场 {market_index} 信息: {e}")
        
        except Exception as e:
            print(f"API调用失败: {e}")
            print("无法通过API验证，请检查网络连接")
        
        # 验证WebSocket订阅配置
        print(f"\n[WebSocket订阅配置]")
        from services.websocket_manager import WebSocketManager
        
        ws_manager = WebSocketManager(config_manager)
        print(f"默认订阅的市场: {list(ws_manager.subscribed_markets)}")
        
        # 检查配置的一致性
        print(f"\n[配置一致性检查]")
        
        btc_pairs = [p for p in trading_pairs if 'btc' in p.name.lower() or 'btc' in p.id.lower()]
        
        if btc_pairs:
            for btc_pair in btc_pairs:
                market_index = btc_pair.market_index
                
                print(f"BTC交易对: {btc_pair.name}")
                print(f"  配置的market_index: {market_index}")
                print(f"  WebSocket是否订阅: {'是' if market_index in ws_manager.subscribed_markets else '否'}")
                print(f"  账户配置: {len(btc_pair.account_addresses)} 个账户")
                print(f"  基础数量: {btc_pair.base_amount}")
                
                if market_index not in ws_manager.subscribed_markets:
                    print(f"  ⚠️  建议: 将market_index {market_index}添加到WebSocket订阅")
        else:
            print("❌ 配置文件中没有找到BTC相关的交易对")
        
        return True
        
    except Exception as e:
        print(f"验证失败: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = asyncio.run(verify_btc_market())
    print(f"\n{'='*50}")
    if success:
        print("✅ BTC市场验证完成")
    else:
        print("❌ BTC市场验证失败")