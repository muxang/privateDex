#!/usr/bin/env python3
"""
测试止盈止损订单的精度计算
"""
import sys
import yaml
from decimal import Decimal

def test_stop_loss_precision():
    """测试止盈止损精度计算"""
    
    print("=== 测试止盈止损精度计算 ===")
    
    # 加载配置
    with open("config/config.yaml", 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    
    # 获取SOL配置
    trading_pair = config['trading_pairs'][0]
    market_index = trading_pair['market_index']
    stop_take_distance = trading_pair['stop_take_distance']
    
    print(f"Market Index: {market_index}")
    print(f"Stop/Take Distance: {stop_take_distance}")
    print(f"Trading Pair: {trading_pair['name']}")
    print()
    
    # 真实SOL精度配置
    price_decimals, size_decimals, price_multiplier, size_multiplier = (3, 3, 1000, 1000)
    print(f"SOL真实精度: price_decimals={price_decimals}, size_decimals={size_decimals}")
    print(f"SOL乘数: price_multiplier={price_multiplier}, size_multiplier={size_multiplier}")
    print()
    
    # 模拟当前SOL价格和仓位
    current_price = Decimal("230.275")  # 当前SOL价格
    position_amount = Decimal("10.5")    # 仓位数量
    entry_price = Decimal("229.800")     # 入场价格
    
    print(f"模拟数据:")
    print(f"  当前价格: {current_price}")
    print(f"  入场价格: {entry_price}")
    print(f"  仓位数量: {position_amount}")
    print()
    
    # 计算止损止盈价格
    price_distance_percent = Decimal(stop_take_distance) / 100  # 0.3% = 0.003
    hedge_lower_price = entry_price * (1 - price_distance_percent)
    hedge_upper_price = entry_price * (1 + price_distance_percent)
    
    print(f"计算结果:")
    print(f"  价格距离百分比: {price_distance_percent}")
    print(f"  下方触发价格 (hedge_lower_price): {hedge_lower_price}")
    print(f"  上方触发价格 (hedge_upper_price): {hedge_upper_price}")
    print()
    
    # 测试Long仓位的止损止盈设置
    print("Long仓位 (买入):")
    sl_trigger_price = hedge_lower_price  # 止损在下方
    tp_trigger_price = hedge_upper_price  # 止盈在上方
    
    print(f"  止损触发价格: {sl_trigger_price}")
    print(f"  止盈触发价格: {tp_trigger_price}")
    
    # 精度转换测试
    base_amount = int(position_amount * size_multiplier)
    sl_trigger_price_int = int(sl_trigger_price * price_multiplier)
    tp_trigger_price_int = int(tp_trigger_price * price_multiplier)
    
    print(f"  转换后的数量: {base_amount}")
    print(f"  转换后的止损价格: {sl_trigger_price_int}")
    print(f"  转换后的止盈价格: {tp_trigger_price_int}")
    
    # 验证精度是否正确
    recovered_sl_price = Decimal(sl_trigger_price_int) / price_multiplier
    recovered_tp_price = Decimal(tp_trigger_price_int) / price_multiplier
    recovered_amount = Decimal(base_amount) / size_multiplier
    
    print()
    print("精度验证:")
    print(f"  原始止损价格: {sl_trigger_price}")
    print(f"  恢复止损价格: {recovered_sl_price}")
    print(f"  精度损失: {abs(sl_trigger_price - recovered_sl_price)}")
    print()
    print(f"  原始止盈价格: {tp_trigger_price}")
    print(f"  恢复止盈价格: {recovered_tp_price}")
    print(f"  精度损失: {abs(tp_trigger_price - recovered_tp_price)}")
    print()
    print(f"  原始数量: {position_amount}")
    print(f"  恢复数量: {recovered_amount}")
    print(f"  精度损失: {abs(position_amount - recovered_amount)}")
    
    # 检查精度损失是否在可接受范围内
    max_acceptable_price_loss = Decimal("0.001")  # 0.001 SOL
    max_acceptable_amount_loss = Decimal("0.001")  # 0.001 个SOL
    
    sl_price_loss = abs(sl_trigger_price - recovered_sl_price)
    tp_price_loss = abs(tp_trigger_price - recovered_tp_price)
    amount_loss = abs(position_amount - recovered_amount)
    
    print()
    print("精度检查结果:")
    print(f"  止损价格精度OK: {sl_price_loss <= max_acceptable_price_loss} (损失: {sl_price_loss})")
    print(f"  止盈价格精度OK: {tp_price_loss <= max_acceptable_price_loss} (损失: {tp_price_loss})")
    print(f"  数量精度OK: {amount_loss <= max_acceptable_amount_loss} (损失: {amount_loss})")
    
    if (sl_price_loss <= max_acceptable_price_loss and 
        tp_price_loss <= max_acceptable_price_loss and 
        amount_loss <= max_acceptable_amount_loss):
        print("\n✅ 精度测试通过！止盈止损应该能正常工作")
    else:
        print("\n❌ 精度测试失败！可能导致订单创建失败")
        
        if sl_price_loss > max_acceptable_price_loss:
            print(f"   - 止损价格精度不足，建议增加price_decimals或调整计算")
        if tp_price_loss > max_acceptable_price_loss:
            print(f"   - 止盈价格精度不足，建议增加price_decimals或调整计算")
        if amount_loss > max_acceptable_amount_loss:
            print(f"   - 数量精度不足，建议增加size_decimals或调整计算")

if __name__ == "__main__":
    test_stop_loss_precision()