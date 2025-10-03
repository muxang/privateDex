#!/usr/bin/env python3
"""
提取真实的市场精度配置
"""
import asyncio
import sys
import lighter
import json

async def extract_real_precision():
    """提取真实的市场精度"""
    try:
        if sys.platform == 'win32':
            import codecs
            sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
        
        print("提取Lighter Protocol真实市场精度...")
        print()
        
        api_client = lighter.ApiClient(lighter.Configuration(host="https://mainnet.zklighter.elliot.ai"))
        order_api = lighter.OrderApi(api_client)
        
        results = {}
        
        for market_index in [1, 2, 3, 4, 5]:
            print(f"分析市场 {market_index}:")
            try:
                # 1. 尝试从order_book_details获取精度
                try:
                    details = await order_api.order_book_details(market_id=market_index)
                    if details and hasattr(details, 'order_book_details'):
                        detail_data = details.order_book_details
                        print(f"  order_book_details类型: {type(detail_data)}")
                        print(f"  order_book_details属性: {dir(detail_data)}")
                        
                        # 检查是否有市场信息
                        if hasattr(detail_data, 'market'):
                            market = detail_data.market
                            price_decimals = getattr(market, 'price_decimals', None)
                            size_decimals = getattr(market, 'size_decimals', None)
                            print(f"  从API获取 - price_decimals: {price_decimals}, size_decimals: {size_decimals}")
                        
                        # 尝试序列化查看完整结构
                        try:
                            detail_dict = detail_data.to_dict() if hasattr(detail_data, 'to_dict') else str(detail_data)
                            print(f"  detail_data内容: {detail_dict}")
                        except:
                            pass
                except Exception as e:
                    print(f"  order_book_details错误: {e}")
                
                # 2. 从订单簿数据分析精度
                try:
                    orderbook = await order_api.order_book_orders(market_id=market_index, limit=10)
                    if orderbook and hasattr(orderbook, 'bids') and hasattr(orderbook, 'asks'):
                        bids = orderbook.bids[:5] if orderbook.bids else []
                        asks = orderbook.asks[:5] if orderbook.asks else []
                        
                        price_samples = []
                        amount_samples = []
                        
                        for order in bids + asks:
                            if hasattr(order, 'price'):
                                price_samples.append(str(order.price))
                            
                            # 检查数量字段
                            if hasattr(order, 'remaining_base_amount'):
                                amount_samples.append(str(order.remaining_base_amount))
                            elif hasattr(order, 'initial_base_amount'):
                                amount_samples.append(str(order.initial_base_amount))
                        
                        if price_samples:
                            # 分析价格精度
                            max_price_decimals = 0
                            for price_str in price_samples:
                                if '.' in price_str:
                                    decimals = len(price_str.split('.')[1].rstrip('0'))
                                    max_price_decimals = max(max_price_decimals, decimals)
                            
                            # 分析数量精度
                            max_amount_decimals = 0
                            for amount_str in amount_samples:
                                if '.' in amount_str:
                                    decimals = len(amount_str.split('.')[1].rstrip('0'))
                                    max_amount_decimals = max(max_amount_decimals, decimals)
                            
                            price_multiplier = 10 ** max_price_decimals
                            amount_multiplier = 10 ** max_amount_decimals
                            
                            results[market_index] = {
                                'price_decimals': max_price_decimals,
                                'size_decimals': max_amount_decimals,
                                'price_multiplier': price_multiplier,
                                'size_multiplier': amount_multiplier,
                                'sample_prices': price_samples[:3],
                                'sample_amounts': amount_samples[:3]
                            }
                            
                            print(f"  从订单簿分析 - 价格精度: {max_price_decimals}, 数量精度: {max_amount_decimals}")
                            print(f"  建议配置: ({max_price_decimals}, {max_amount_decimals}, {price_multiplier}, {amount_multiplier})")
                            print(f"  价格样本: {price_samples[:3]}")
                            print(f"  数量样本: {amount_samples[:3]}")
                        
                except Exception as e:
                    print(f"  订单簿分析错误: {e}")
                
                print()
                
            except Exception as e:
                print(f"  市场{market_index}分析失败: {e}")
                print()
        
        print("=" * 80)
        print("真实精度配置总结:")
        print("=" * 80)
        
        for market_index, config in results.items():
            print(f"Market {market_index}: ({config['price_decimals']}, {config['size_decimals']}, {config['price_multiplier']}, {config['size_multiplier']})")
        
        print()
        print("建议更新order_manager.py中的精度配置:")
        for market_index, config in results.items():
            if market_index == 1:
                currency = "BTC"
            elif market_index == 2:
                currency = "SOL"
            else:
                currency = f"市场{market_index}"
            
            print(f"elif market_index == {market_index}:  # {currency}")
            print(f"    return ({config['price_decimals']}, {config['size_decimals']}, {config['price_multiplier']}, {config['size_multiplier']})")
        
    except Exception as e:
        print(f"提取失败: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(extract_real_precision())