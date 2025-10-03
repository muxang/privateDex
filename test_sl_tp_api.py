#!/usr/bin/env python3
"""
测试止盈止损API调用
"""
import asyncio
import sys
import lighter
from decimal import Decimal

async def test_sl_tp_api():
    """测试止盈止损API方法是否存在"""
    try:
        if sys.platform == 'win32':
            import codecs
            sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
        
        print("测试Lighter SDK止盈止损API方法...")
        print()
        
        # 创建API客户端
        api_client = lighter.ApiClient(lighter.Configuration(host="https://mainnet.zklighter.elliot.ai"))
        
        # 检查是否有SignerClient类
        try:
            from lighter import SignerClient
            print("✓ SignerClient类存在")
            
            # 检查SignerClient的方法
            methods = [method for method in dir(SignerClient) if not method.startswith('_')]
            print(f"SignerClient可用方法: {len(methods)}个")
            
            # 检查止盈止损相关方法
            sl_tp_methods = [method for method in methods if 'sl_' in method or 'tp_' in method or 'stop' in method or 'take' in method]
            print(f"止盈止损相关方法: {sl_tp_methods}")
            
            # 检查具体的方法
            required_methods = ['create_sl_order', 'create_tp_order', 'create_sl_limit_order', 'create_tp_limit_order']
            for method in required_methods:
                if hasattr(SignerClient, method):
                    print(f"✓ {method} 方法存在")
                else:
                    print(f"✗ {method} 方法不存在")
            
        except ImportError as e:
            print(f"✗ SignerClient导入失败: {e}")
        
        # 检查OrderApi的方法
        try:
            order_api = lighter.OrderApi(api_client)
            methods = [method for method in dir(order_api) if not method.startswith('_')]
            print(f"\nOrderApi可用方法: {len(methods)}个")
            
            # 查找订单相关方法
            order_methods = [method for method in methods if 'order' in method.lower()]
            print(f"订单相关方法: {order_methods}")
            
        except Exception as e:
            print(f"OrderApi创建失败: {e}")
        
        # 检查其他可能的API类
        try:
            # 检查所有可用的API类
            api_classes = [attr for attr in dir(lighter) if 'Api' in attr and not attr.startswith('_')]
            print(f"\n可用API类: {api_classes}")
            
            for api_class_name in api_classes:
                try:
                    api_class = getattr(lighter, api_class_name)
                    api_instance = api_class(api_client)
                    methods = [method for method in dir(api_instance) if not method.startswith('_') and callable(getattr(api_instance, method))]
                    
                    # 查找止盈止损相关方法
                    sl_tp_methods = [method for method in methods if any(keyword in method.lower() for keyword in ['sl_', 'tp_', 'stop', 'take', 'profit', 'loss'])]
                    
                    if sl_tp_methods:
                        print(f"\n{api_class_name} 止盈止损方法: {sl_tp_methods}")
                
                except Exception as e:
                    continue
                    
        except Exception as e:
            print(f"检查API类失败: {e}")
        
    except Exception as e:
        print(f"测试失败: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_sl_tp_api())