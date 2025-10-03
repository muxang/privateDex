#!/usr/bin/env python3
"""
调试API响应结构
"""
import asyncio
import sys
import lighter
import json

async def debug_api_structure():
    """调试API响应的数据结构"""
    try:
        if sys.platform == 'win32':
            import codecs
            sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
        
        print("调试Lighter Protocol API响应结构...")
        print()
        
        api_client = lighter.ApiClient(lighter.Configuration(host="https://mainnet.zklighter.elliot.ai"))
        order_api = lighter.OrderApi(api_client)
        
        # 测试market_index=2 (SOL)
        market_index = 2
        print(f"测试market_index={market_index}的API响应:")
        print("=" * 60)
        
        try:
            # 方法1: order_book_details
            print("1. 测试order_book_details:")
            try:
                details = await order_api.order_book_details(market_id=market_index)
                if details:
                    print(f"  响应类型: {type(details)}")
                    print(f"  响应属性: {dir(details)}")
                    if hasattr(details, 'market'):
                        market = details.market
                        print(f"  市场属性: {dir(market)}")
                        print(f"  price_decimals: {getattr(market, 'price_decimals', 'N/A')}")
                        print(f"  size_decimals: {getattr(market, 'size_decimals', 'N/A')}")
                else:
                    print("  无响应数据")
            except Exception as e:
                print(f"  错误: {e}")
            
            print()
            
            # 方法2: order_book_orders
            print("2. 测试order_book_orders:")
            try:
                orderbook = await order_api.order_book_orders(market_id=market_index, limit=5)
                if orderbook:
                    print(f"  响应类型: {type(orderbook)}")
                    print(f"  响应属性: {dir(orderbook)}")
                    
                    if hasattr(orderbook, 'bids'):
                        bids = orderbook.bids
                        print(f"  bids类型: {type(bids)}")
                        if bids and len(bids) > 0:
                            print(f"  第一个bid类型: {type(bids[0])}")
                            print(f"  第一个bid属性: {dir(bids[0])}")
                            print(f"  第一个bid价格: {getattr(bids[0], 'price', 'N/A')}")
                            print(f"  第一个bid数量: {getattr(bids[0], 'size', 'N/A')}")
                    
                    if hasattr(orderbook, 'asks'):
                        asks = orderbook.asks
                        print(f"  asks类型: {type(asks)}")
                        if asks and len(asks) > 0:
                            print(f"  第一个ask类型: {type(asks[0])}")
                            print(f"  第一个ask属性: {dir(asks[0])}")
                            print(f"  第一个ask价格: {getattr(asks[0], 'price', 'N/A')}")
                            print(f"  第一个ask数量: {getattr(asks[0], 'size', 'N/A')}")
                else:
                    print("  无响应数据")
            except Exception as e:
                print(f"  错误: {e}")
            
            print()
            
            # 方法3: 尝试其他API方法
            print("3. 尝试其他可能的API方法:")
            api_methods = [method for method in dir(order_api) if not method.startswith('_') and callable(getattr(order_api, method))]
            print(f"  可用方法: {api_methods}")
            
        except Exception as e:
            print(f"测试失败: {e}")
            import traceback
            traceback.print_exc()
        
    except Exception as e:
        print(f"初始化失败: {e}")

if __name__ == "__main__":
    asyncio.run(debug_api_structure())