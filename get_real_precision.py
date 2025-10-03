#!/usr/bin/env python3
"""
从Lighter Protocol API获取真实的市场精度配置
"""
import asyncio
import sys
import lighter
import json

async def get_real_market_precision():
    """获取真实的市场精度配置"""
    try:
        if sys.platform == 'win32':
            import codecs
            sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
        
        print("获取Lighter Protocol真实市场精度配置...")
        print()
        
        api_client = lighter.ApiClient(lighter.Configuration(host="https://mainnet.zklighter.elliot.ai"))
        order_api = lighter.OrderApi(api_client)
        
        print("分析市场1-10的真实精度要求:")
        print("-" * 120)
        print(f"{'Index':<6} {'Price':<12} {'Price Analysis':<25} {'Size Analysis':<25} {'Suggested Precision':<25}")
        print("-" * 120)
        
        for market_index in range(1, 11):
            try:
                # 获取订单簿数据
                orderbook = await order_api.order_book_orders(market_id=market_index, limit=20)
                
                if orderbook and hasattr(orderbook, 'bids') and hasattr(orderbook, 'asks'):
                    bids = orderbook.bids if orderbook.bids else []
                    asks = orderbook.asks if orderbook.asks else []
                    
                    if len(bids) > 0 and len(asks) > 0:
                        # 分析价格精度
                        price_samples = []
                        size_samples = []
                        
                        # 收集价格和数量样本
                        for bid in bids[:10]:
                            if hasattr(bid, 'price') and hasattr(bid, 'size'):
                                price_samples.append(str(bid.price))
                                size_samples.append(str(bid.size))
                        
                        for ask in asks[:10]:
                            if hasattr(ask, 'price') and hasattr(ask, 'size'):
                                price_samples.append(str(ask.price))
                                size_samples.append(str(ask.size))
                        
                        if price_samples and size_samples:
                            # 分析价格精度
                            price_decimals = 0
                            for price_str in price_samples:
                                if '.' in price_str:
                                    decimals = len(price_str.split('.')[1])
                                    price_decimals = max(price_decimals, decimals)
                            
                            # 分析数量精度
                            size_decimals = 0
                            for size_str in size_samples:
                                if '.' in size_str:
                                    decimals = len(size_str.split('.')[1])
                                    size_decimals = max(size_decimals, decimals)
                            
                            # 计算建议的乘数
                            price_multiplier = 10 ** price_decimals
                            size_multiplier = 10 ** size_decimals
                            
                            price_range = f"{min([float(p) for p in price_samples]):.{price_decimals}f}-{max([float(p) for p in price_samples]):.{price_decimals}f}"
                            size_range = f"{min([float(s) for s in size_samples]):.{size_decimals}f}-{max([float(s) for s in size_samples]):.{size_decimals}f}"
                            
                            suggested_precision = f"({price_decimals},{size_decimals},{price_multiplier},{size_multiplier})"
                            
                            print(f"{market_index:<6} {price_range:<12} {'精度:'+str(price_decimals)+'位':<25} {'精度:'+str(size_decimals)+'位':<25} {suggested_precision:<25}")
                        else:
                            print(f"{market_index:<6} {'N/A':<12} {'无数据':<25} {'无数据':<25} {'无法确定':<25}")
                    else:
                        print(f"{market_index:<6} {'N/A':<12} {'订单簿为空':<25} {'订单簿为空':<25} {'无法确定':<25}")
                else:
                    print(f"{market_index:<6} {'N/A':<12} {'无订单数据':<25} {'无订单数据':<25} {'无法确定':<25}")
                    
            except Exception as e:
                error_msg = str(e)[:20] + "..." if len(str(e)) > 20 else str(e)
                print(f"{market_index:<6} {'ERROR':<12} {error_msg:<25} {'':<25} {'查询失败':<25}")
        
        print("-" * 120)
        print()
        print("建议的精度配置更新:")
        print("在order_manager.py的_infer_precision_from_pair_name方法中使用上述真实精度")
        
    except Exception as e:
        print(f"查询失败: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(get_real_market_precision())