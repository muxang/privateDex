#!/usr/bin/env python3
"""
获取Lighter Protocol实际市场信息 - 通过订单簿数据推断
"""
import asyncio
import sys
import lighter
from datetime import datetime

async def get_actual_markets():
    """通过订单簿数据获取实际市场信息"""
    try:
        if sys.platform == 'win32':
            import codecs
            sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
        
        print("通过订单簿获取Lighter Protocol实际市场信息...")
        print(f"查询时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print()
        
        api_client = lighter.ApiClient(lighter.Configuration(host="https://mainnet.zklighter.elliot.ai"))
        order_api = lighter.OrderApi(api_client)
        
        print("分析市场索引 1-10 的订单簿数据:")
        print("-" * 100)
        print(f"{'Index':<8} {'Status':<12} {'Best Bid':<15} {'Best Ask':<15} {'Spread':<10} {'Analysis':<30}")
        print("-" * 100)
        
        for market_index in range(1, 11):
            try:
                # 获取订单簿数据
                orderbook = await order_api.order_book_orders(market_id=market_index, limit=10)
                
                if orderbook and hasattr(orderbook, 'bids') and hasattr(orderbook, 'asks'):
                    bids = orderbook.bids if orderbook.bids else []
                    asks = orderbook.asks if orderbook.asks else []
                    
                    if len(bids) > 0 and len(asks) > 0:
                        best_bid = float(bids[0].price) if hasattr(bids[0], 'price') else 0
                        best_ask = float(asks[0].price) if hasattr(asks[0], 'price') else 0
                        
                        if best_bid > 0 and best_ask > 0:
                            spread = best_ask - best_bid
                            spread_pct = (spread / best_bid) * 100 if best_bid > 0 else 0
                            
                            # 根据价格范围推断可能的币种
                            analysis = ""
                            if best_bid > 50000:  # 可能是BTC
                                analysis = "可能是BTC (高价)"
                            elif 2000 <= best_bid <= 5000:  # 可能是ETH
                                analysis = "可能是ETH (中价)"
                            elif 100 <= best_bid <= 300:  # 可能是SOL
                                analysis = "可能是SOL (中低价)"
                            elif best_bid < 5:  # 可能是稳定币或小币
                                analysis = "可能是稳定币/小币"
                            else:
                                analysis = f"未知币种 (价格范围)"
                            
                            print(f"{market_index:<8} {'ACTIVE':<12} {best_bid:<15.2f} {best_ask:<15.2f} {spread_pct:<10.3f}% {analysis:<30}")
                        else:
                            print(f"{market_index:<8} {'INVALID':<12} {'N/A':<15} {'N/A':<15} {'N/A':<10} {'价格数据异常':<30}")
                    else:
                        print(f"{market_index:<8} {'EMPTY':<12} {'N/A':<15} {'N/A':<15} {'N/A':<10} {'订单簿为空':<30}")
                else:
                    print(f"{market_index:<8} {'NO_ORDERS':<12} {'N/A':<15} {'N/A':<15} {'N/A':<10} {'无订单数据':<30}")
                    
            except Exception as e:
                error_msg = str(e)[:25] + "..." if len(str(e)) > 25 else str(e)
                print(f"{market_index:<8} {'ERROR':<12} {'N/A':<15} {'N/A':<15} {'N/A':<10} {error_msg:<30}")
        
        print("-" * 100)
        print("市场分析完成")
        print()
        print("建议:")
        print("1. 根据价格分析结果推断各market_index对应的币种")
        print("2. 将market_index=2的实际币种更新到你的配置中")
        print("3. 如果你想交易ETH，找到对应ETH价格范围的market_index")
        
    except Exception as e:
        print(f"查询失败: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(get_actual_markets())