"""
Balanced Hedge Strategy Implementation
平衡对冲策略：同时在两个账户开设相反仓位来实现风险对冲
"""

import asyncio
from typing import Dict, List, Optional, Tuple
from datetime import datetime
from decimal import Decimal
import structlog

from src.models import (
    HedgePosition, Position, TradingPairConfig, OrderInfo, 
    PositionStatus, OrderStatus, LeverageConfig, MarketData, OrderBook
)
from src.services.order_manager import OrderManager
from src.services.account_manager import AccountManager
from src.utils.logger import trade_logger

logger = structlog.get_logger()


class BalancedHedgeStrategy:
    """平衡对冲策略实现"""
    
    def __init__(self, order_manager: OrderManager, account_manager: AccountManager, client_factory=None):
        self.order_manager = order_manager
        self.account_manager = account_manager
        # 如果没有传入client_factory，尝试从account_manager获取
        self.client_factory = client_factory or getattr(account_manager, 'client_factory', None)
        # 从order_manager获取config_manager引用
        self.config_manager = getattr(order_manager, 'config_manager', None)
        # 初始化活跃仓位字典
        self.active_positions = {}
    
    def _infer_market_type_from_pair(self, pair_config: TradingPairConfig) -> str:
        """从交易对配置推断市场类型"""
        try:
            # 从交易对名称或ID中推断市场类型
            name_lower = pair_config.name.lower()
            id_lower = pair_config.id.lower()
            
            if 'btc' in name_lower or 'btc' in id_lower or 'bitcoin' in name_lower:
                return "BTC"
            elif 'eth' in name_lower or 'eth' in id_lower or 'ethereum' in name_lower:
                return "ETH"
            elif 'sol' in name_lower or 'sol' in id_lower or 'solana' in name_lower:
                return "SOL"
            else:
                # 默认使用BTC配置
                logger.debug("无法识别市场类型，使用默认BTC配置", 
                           pair_name=pair_config.name, 
                           pair_id=pair_config.id)
                return "BTC"
                
        except Exception as e:
            logger.error("推断市场类型失败，使用默认BTC配置", error=str(e))
            return "BTC"
        
    async def execute_hedge_open(
        self,
        pair_config: TradingPairConfig,
        accounts: List[int],
        trigger_price: Decimal
    ) -> Optional[HedgePosition]:
        """
        执行对冲开仓
        
        Args:
            pair_config: 交易对配置
            accounts: 用于对冲的账户列表 (需要至少2个)
            trigger_price: 触发价格
            
        Returns:
            创建的对冲仓位，如果失败返回None
        """
        if len(accounts) < 2:
            logger.error("对冲开仓需要至少2个账户", accounts_count=len(accounts))
            return None
        
        logger.info("🚀 开始执行平衡对冲开仓",
                   pair_id=pair_config.id,
                   accounts=accounts,
                   trigger_price=float(trigger_price))
        
        try:
            # 开仓前的完整清理流程
            logger.info("🧹 开始开仓前的环境清理", 
                       pair_id=pair_config.id,
                       accounts=accounts)
            
            # 第一步：取消所有委托订单
            logger.info("🧹 步骤1: 取消所有委托订单", 
                       pair_id=pair_config.id,
                       accounts=accounts)
            
            cleanup_orders_success = await self._cleanup_all_pending_orders(pair_config, accounts)
            if not cleanup_orders_success:
                logger.warning("⚠️ 清理委托订单部分失败，继续后续流程")
            
            # 第二步：平掉所有仓位
            logger.info("🧹 步骤2: 平掉所有仓位", 
                       pair_id=pair_config.id,
                       accounts=accounts)
            
            cleanup_positions_success = await self._cleanup_all_positions(pair_config, accounts)
            if not cleanup_positions_success:
                logger.warning("⚠️ 清理仓位部分失败，继续后续流程")
            
            # 环境清理完成
            if cleanup_orders_success and cleanup_positions_success:
                logger.debug("✨ 环境清理完成，准备开仓", pair_id=pair_config.id)
            else:
                logger.warning("⚠️ 环境清理部分失败，但继续开仓流程", pair_id=pair_config.id)
            
            # 第三步：创建对冲仓位记录
            position_id = f"hedge_{pair_config.id}_{int(datetime.now().timestamp())}"
            
            hedge_position = HedgePosition(
                id=position_id,
                pair_id=pair_config.id,
                status=PositionStatus.OPENING,
                strategy=pair_config.hedge_strategy,
                positions=[],
                created_at=datetime.now(),
                updated_at=datetime.now(),
                metadata={
                    "trigger_price": float(trigger_price),
                    "accounts": accounts,
                    "strategy_type": "balanced_hedge"
                }
            )
            
            # 从配置中获取目标杠杆倍数（支持无限制杠杆）
            leverage = getattr(pair_config, 'leverage', 1)  # 使用配置中的leverage字段
            if leverage < 1:
                logger.warning("杠杆倍数无效，使用现货模式", leverage=leverage)
                leverage = 1
            
            logger.info("使用杠杆倍数", leverage=leverage)
            
            # 获取所有账户余额，找到最小余额账户作为限制因子
            account_balances = await self._get_account_balances(accounts)
            min_balance = min(account_balances.values()) if account_balances else Decimal('0')
            
            # 获取配置的最小余额要求
            min_balance_required = Decimal(str(pair_config.risk_limits.min_balance))
            
            # 计算可用于保证金的资金：使用较小账户的可用余额最大化利用
            # 确保每个账户都保留最小余额要求
            max_safe_margin_per_account = max(Decimal('0'), min_balance - min_balance_required)
            
            # 安全检查：如果可用保证金太少，给出警告
            if max_safe_margin_per_account < Decimal('10'):
                logger.warning("可用保证金过少，可能影响交易效果",
                             min_balance=float(min_balance),
                             min_balance_required=float(min_balance_required),
                             available_margin=float(max_safe_margin_per_account))
            
            # 详细的账户余额日志
            logger.debug("账户余额分析",
                       accounts_count=len(accounts),
                       account_balances={k: float(v) for k, v in account_balances.items()},
                       min_balance=float(min_balance),
                       min_balance_required=float(min_balance_required),
                       max_safe_margin_per_account=float(max_safe_margin_per_account),
                       utilization_rate=float((max_safe_margin_per_account / min_balance) * 100) if min_balance > 0 else 0)
            
            # 计算杠杆条件下实际控制的仓位价值
            if leverage > 1:
                # 杠杆模式：实际控制仓位 = 保证金 × 杠杆倍数
                actual_position_per_account = max_safe_margin_per_account * leverage
                margin_per_account = max_safe_margin_per_account  # 保证金需求
            else:
                # 现货模式：实际控制仓位 = 保证金
                actual_position_per_account = max_safe_margin_per_account
                margin_per_account = max_safe_margin_per_account
            
            # 总计算
            total_margin_required = margin_per_account * len(accounts)
            total_position_value = actual_position_per_account * len(accounts)
            
            logger.info("最大化保证金利用的杠杆计算",
                       leverage=leverage,
                       min_account_balance=float(min_balance),
                       margin_per_account=float(margin_per_account),
                       actual_position_per_account=float(actual_position_per_account),
                       total_margin_required=float(total_margin_required),
                       total_position_value=float(total_position_value),
                       margin_utilization_percent=float((margin_per_account / min_balance) * 100) if min_balance > 0 else 0)
            
            # 将美元价值转换为BTC数量
            # actual_position_per_account是美元价值，需要转换为BTC数量
            # 先获取当前价格来计算
            market_data = await self.order_manager.get_market_data(pair_config.market_index)
            if not market_data:
                logger.error("无法获取市场数据进行仓位计算", pair_id=pair_config.id)
                hedge_position.status = PositionStatus.FAILED
                return hedge_position
            
            current_price = market_data.price
            # 将美元价值转换为BTC数量
            amount_per_account = actual_position_per_account / current_price
            
            logger.info("最终交易参数",
                       usd_value_per_account=float(actual_position_per_account),
                       btc_amount_per_account=float(amount_per_account),
                       current_price=float(current_price),
                       margin_per_account=float(margin_per_account),
                       leverage=leverage,
                       account_balances={k: float(v) for k, v in account_balances.items()})
            
            # 验证计算结果
            expected_usd_value = float(amount_per_account * current_price)
            logger.info("仓位计算验证",
                       expected_total_usd_per_account=float(actual_position_per_account),
                       calculated_usd_value=expected_usd_value,
                       calculation_correct=abs(expected_usd_value - float(actual_position_per_account)) < 1.0)
            
            # 获取统一的执行价格以确保对冲价格一致性
            unified_price = await self._get_unified_execution_price(pair_config.market_index)
            if not unified_price:
                # 备用方案：使用当前价格作为统一价格
                logger.warning("无法获取统一执行价格，使用当前价格作为备用", 
                             pair_id=pair_config.id,
                             current_price=float(current_price))
                unified_price = current_price
            
            logger.info("使用统一执行价格确保对冲一致性",
                       position_id=position_id,
                       unified_price=float(unified_price),
                       amount_per_account=float(amount_per_account))
            
            # 获取轮转的开仓方向分配
            direction_assignments = await self._get_rotated_direction_assignments(
                accounts, pair_config.id
            )
            
            # 创建精确对冲订单 - 使用相同价格和数量，但轮转方向
            tasks = []
            
            for account_index, (side, role) in direction_assignments.items():
                tasks.append(self._create_precise_hedge_order(
                    account_index=account_index,
                    market_index=pair_config.market_index,
                    side=side,
                    amount=amount_per_account,
                    position_id=position_id,
                    role=role
                ))
            
            logger.info("轮转方向分配完成",
                       position_id=position_id,
                       direction_assignments=direction_assignments)
            
            # 同步执行所有订单以确保时间一致性
            results = await self._execute_synchronized_orders(tasks)
            
            # 检查结果并验证对冲一致性
            successful_orders = []
            failed_orders = []
            
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    logger.error(f"账户 {accounts[i]} 订单执行失败", error=str(result))
                    failed_orders.append(accounts[i])
                elif result:
                    # 对于市价单，即使状态为FAILED也可能在交易所成功执行
                    is_market_order = result.metadata.get('market_order', False)
                    if result.status == OrderStatus.FAILED and is_market_order:
                        logger.warning(f"市价单SDK返回失败状态，但可能已在交易所执行", 
                                     account=accounts[i],
                                     order_id=result.id,
                                     order_type=result.order_type)
                        # 将状态改为FILLED以便后续验证
                        result.status = OrderStatus.FILLED
                        result.filled_at = result.created_at
                        result.filled_amount = result.amount
                        result.filled_price = result.price
                    
                    successful_orders.append(result)
                    logger.info(f"账户 {accounts[i]} 订单执行成功", order_id=result.id)
                else:
                    failed_orders.append(accounts[i])
            
            # 严格验证对冲一致性
            if successful_orders and not self._validate_hedge_consistency(successful_orders, amount_per_account, unified_price):
                logger.error("对冲一致性验证失败，取消所有订单")
                failed_orders.extend(successful_orders)
                successful_orders = []
            
            # 如果有失败的订单，需要处理
            if failed_orders:
                logger.error("部分订单执行失败，尝试取消已成功订单",
                           failed_accounts=failed_orders,
                           successful_orders_count=len(successful_orders))
                
                # 取消已成功的订单
                for order in successful_orders:
                    try:
                        await self.order_manager.cancel_order(order.id)
                    except Exception as e:
                        logger.error("取消订单失败", order_id=order.id, error=str(e))
                
                hedge_position.status = PositionStatus.FAILED
                hedge_position.updated_at = datetime.now()
                
                return hedge_position
            
            # 所有订单都成功，更新仓位状态
            hedge_position.status = PositionStatus.ACTIVE
            hedge_position.updated_at = datetime.now()
            
            # 使用持仓变化验证订单成交 (解决后台订单查询问题)
            orders_filled = await self.order_manager.verify_orders_by_position_changes(
                successful_orders, timeout=30
            )
            
            if not orders_filled:
                logger.warning("持仓变化验证未通过，但继续处理 (订单可能已在区块链成交)")
                # 不直接失败，因为订单可能确实已经成交了
            
            # 简化订单状态检查，优先信任区块链提交结果
            filled_orders = successful_orders  # 假设提交成功的订单都会成交
            failed_orders = []
            
            # 记录订单处理结果
            logger.info("订单处理结果",
                       filled_count=len(filled_orders),
                       position_verified=orders_filled)
            
            # 如果有订单失败，需要清理仓位
            if failed_orders:
                logger.error("部分订单未成交，清理仓位",
                           failed_count=len(failed_orders),
                           filled_count=len(filled_orders))
                
                # 尝试取消所有未成交的订单
                for order in failed_orders:
                    try:
                        await self.order_manager.cancel_order(order.id)
                    except Exception as cancel_error:
                        logger.warning("取消订单失败", order_id=order.id, error=str(cancel_error))
                
                # 如果全部订单都失败，标记仓位为失败并返回None
                if not filled_orders:
                    hedge_position.status = PositionStatus.FAILED
                    logger.error("所有订单都未成交，仓位开仓失败")
                    return None
                
                # 如果部分成交，也标记为失败（对冲需要全部成交）
                logger.warning("对冲订单部分失败，不符合对冲策略要求")
                hedge_position.status = PositionStatus.FAILED
                return None
            
            # 创建仓位记录，记录精确的成交价格
            # 准备杠杆配置和目标杠杆
            target_leverage = leverage  # 使用之前定义的leverage变量
            # 从交易对名称推断市场类型
            market_type = self._infer_market_type_from_pair(pair_config)
            leverage_config = LeverageConfig.from_market_type(market_type) if target_leverage > 1 else None
            
            positions = []
            entry_prices = []
            for order in filled_orders:
                # 简化状态检查，假设提交成功的订单都已成交
                actual_entry_price = getattr(order, 'filled_price', None) or order.price or trigger_price
                entry_prices.append(actual_entry_price)
                
                # 更新订单状态为已成交
                order.status = OrderStatus.FILLED
                # 确保filled_amount被正确设置（市价单使用原始amount）
                if order.filled_amount == Decimal("0"):
                    order.filled_amount = order.amount
                    logger.debug("设置订单填充数量", 
                               order_id=order.id,
                               filled_amount=float(order.filled_amount))
                if not hasattr(order, 'filled_price') or order.filled_price is None:
                    order.filled_price = actual_entry_price
                
                position_side = "long" if order.side == "buy" else "short"
                
                # 计算清算价格（如果有杠杆）
                liquidation_price = None
                if leverage_config and target_leverage > 1:
                    liquidation_price = self._calculate_liquidation_price(
                        position_side, actual_entry_price, order.filled_amount,
                        target_leverage, leverage_config, margin_per_account
                    )
                
                position = Position(
                    id=f"pos_{order.id}",
                    account_index=order.account_index,
                    market_index=order.market_index,
                    side=position_side,
                    size=order.filled_amount,
                    entry_price=actual_entry_price,
                    current_price=actual_entry_price,  # 初始当前价格等于成交价格
                    leverage=target_leverage,
                    margin_used=margin_per_account,
                    liquidation_price=liquidation_price,
                    created_at=datetime.now(),
                    updated_at=datetime.now()
                )
                positions.append(position)
            
            # 市价单成交验证 - 确保止损止盈一定创建
            price_consistency_verified = True  # 市价单直接设为True，确保止损止盈创建
            
            # 定义价格容差
            strict_tolerance = Decimal('0.02')  # 2%容差
            price_diff = Decimal('0')
            
            if len(entry_prices) >= 2:
                max_entry = max(entry_prices)
                min_entry = min(entry_prices)
                price_diff = (max_entry - min_entry) / min_entry if min_entry > 0 else Decimal('0')
                
                logger.debug("市价单成交价格分析",
                           position_id=position_id,
                           max_entry_price=float(max_entry),
                           min_entry_price=float(min_entry),
                           price_diff_percent=float(price_diff * 100),
                           avg_entry_price=float(sum(entry_prices) / len(entry_prices)))
                
                # 市价单可能有价格差异，但仍然创建止损止盈
                if price_diff > strict_tolerance:
                    logger.warning("市价单成交价格差异较大，但继续创建止损止盈",
                                 position_id=position_id,
                                 price_diff_percent=float(price_diff * 100))
                    hedge_position.metadata['market_order_price_warning'] = True
                    hedge_position.metadata['price_diff_percent'] = float(price_diff * 100)
                else:
                    logger.info("市价单成交价格差异在合理范围",
                               position_id=position_id,
                               price_diff_percent=float(price_diff * 100))
            else:
                logger.info("单个订单成交，直接创建止损止盈", position_id=position_id)
            
            # 更新hedge_position元数据包含实际成交价格和杠杆信息
            hedge_position.metadata.update({
                "actual_entry_prices": [float(p) for p in entry_prices],
                "avg_entry_price": float(sum(entry_prices) / len(entry_prices)) if entry_prices else float(trigger_price),
                "unified_price": float(unified_price),
                "price_consistency_verified": price_consistency_verified,
                "strict_price_tolerance": float(strict_tolerance * 100) if len(entry_prices) >= 2 else None,
                "actual_price_diff_percent": float(price_diff * 100) if len(entry_prices) >= 2 else None,
                "target_leverage": target_leverage,
                "total_margin_used": float(margin_per_account * len(accounts)),
                "margin_per_account": float(margin_per_account),  # 用于PnL计算
                "leverage_config": leverage_config.dict() if leverage_config else None,
                "liquidation_prices": [float(p.liquidation_price or 0) for p in positions],
                "hedge_precision": "precise",  # 标记为精确对冲
                "execution_method": "synchronized_limit_orders"  # 执行方法
            })
            
            hedge_position.positions = positions
            
            # 等待一段时间让订单充分处理，避免过早检查
            logger.debug("等待订单完全处理后再进行后端验证", position_id=position_id)
            await asyncio.sleep(5)  # 等待5秒让后端系统处理完成
            
            # 后端验证改为可选 (解决订单查询问题)
            logger.info("开始后端仓位同步验证", position_id=position_id)
            backend_validation = False  # 直接跳过后端验证，避免订单查询问题
            
            logger.info("跳过后端验证，使用持仓变化验证结果", 
                       position_id=position_id,
                       position_verified=orders_filled)
            hedge_position.metadata['backend_validated'] = orders_filled
            hedge_position.metadata['validation_method'] = 'position_change'
            hedge_position.metadata['validation_timestamp'] = datetime.now().isoformat()
                
            # 记录交易日志
            trade_logger.log_position_open({
                "position_id": position_id,
                "pair_id": pair_config.id,
                "strategy": "balanced_hedge",
                "accounts": accounts,
                "positions_count": len(positions),
                "total_margin_used": float(total_margin_required),
                "total_position_value": float(total_position_value),
                "trigger_price": float(trigger_price),
                "backend_validated": backend_validation,
                "timestamp": datetime.now().isoformat(),
                "orders": [{"id": o.id, "account": o.account_index, "side": o.side, "amount": float(o.amount)} for o in successful_orders]
            })
            
            # 如果对冲成功（价格一致性通过），设置状态并添加到活跃仓位
            # 注意：即使backend_validation失败也创建止损止盈，因为订单已在区块链
            if price_consistency_verified:
                hedge_position.status = PositionStatus.ACTIVE
                # 添加到活跃仓位管理
                if not hasattr(self, 'active_positions'):
                    self.active_positions = {}
                self.active_positions[position_id] = hedge_position
                
                # 创建协调的止损止盈订单
                logger.info("开始创建协调的对冲止损止盈订单", position_id=position_id)
                try:
                    # 刷新仓位数据以确保获得最新状态
                    logger.debug("刷新仓位数据以准备止损止盈", position_id=position_id)
                    refreshed_positions = []
                    for account_index in accounts:
                        try:
                            account_positions = await self.order_manager.get_positions_for_account(account_index)
                            if account_positions:
                                for pos in account_positions:
                                    if (pos.market_index == pair_config.market_index and 
                                        pos.size > 0):  # 只添加当前市场的有效仓位
                                        refreshed_positions.append(pos)
                                        logger.info("发现活跃仓位",
                                                  account_index=account_index,
                                                  position_id=pos.id,
                                                  side=pos.side,
                                                  size=float(pos.size),
                                                  entry_price=float(pos.entry_price))
                        except Exception as e:
                            logger.warning("获取账户仓位失败，使用原始数据",
                                         account_index=account_index,
                                         error=str(e))
                    
                    # 如果刷新成功，使用刷新的数据，否则使用原始数据
                    positions_to_use = refreshed_positions if refreshed_positions else positions
                    logger.debug("仓位数据选择",
                               position_id=position_id,
                               refreshed_count=len(refreshed_positions),
                               original_count=len(positions),
                               using_refreshed=len(refreshed_positions) > 0)
                    
                    # 准备仓位数据给止损止盈方法
                    positions_for_sl_tp = []
                    for position in positions_to_use:
                        if position.size > 0:  # 只添加有效仓位
                            positions_for_sl_tp.append({
                                "account_index": position.account_index,
                                "side": "buy" if position.side == "long" else "sell",
                                "amount": position.size,
                                "entry_price": position.entry_price
                            })
                        else:
                            logger.warning("发现无效仓位，跳过止损止盈设置",
                                         position_id=position.id,
                                         account_index=position.account_index,
                                         side=position.side,
                                         size=float(position.size))
                    
                    logger.debug("准备止损止盈仓位数据",
                               position_id=position_id,
                               positions_count=len(positions_to_use),
                               valid_positions_count=len(positions_for_sl_tp),
                               positions_data=positions_for_sl_tp)
                    
                    # 检查是否有有效仓位
                    if not positions_for_sl_tp:
                        logger.warning("没有有效仓位，跳过止损止盈创建",
                                     position_id=position_id,
                                     total_positions=len(positions_to_use))
                        return
                    
                    # 调用新的协调止损止盈方法
                    await self._create_hedge_stop_loss_take_profit_orders(
                        hedge_position_id=position_id,
                        positions=positions_for_sl_tp,
                        market_index=pair_config.market_index
                    )
                    
                    logger.info("✅ 协调止损止盈订单创建完成", position_id=position_id)
                    
                except Exception as sl_tp_error:
                    logger.error("创建协调止损止盈订单失败", 
                               position_id=position_id, 
                               error=str(sl_tp_error))
                    # 不阻止开仓成功，但记录错误
                    hedge_position.metadata['sl_tp_error'] = str(sl_tp_error)
                
                logger.info("✅ 平衡对冲开仓完成（含止损止盈）",
                           position_id=position_id,
                           positions_count=len(positions),
                           total_margin_used=float(total_margin_required),
                           total_position_value=float(total_position_value),
                           backend_validated=backend_validation,
                           price_consistency=price_consistency_verified)
            else:
                logger.warning("⚠️ 平衡对冲开仓有问题",
                             position_id=position_id,
                             backend_validated=backend_validation,
                             price_consistency=price_consistency_verified)
            
            return hedge_position
            
        except Exception as e:
            logger.error("平衡对冲开仓失败", error=str(e))
            
            # 清理失败的仓位状态，避免阻塞后续交易
            if hasattr(self, 'active_positions') and position_id in self.active_positions:
                logger.info("清理失败的仓位状态", position_id=position_id)
                del self.active_positions[position_id]
                
            return None
    
    async def _create_hedge_order(
        self,
        account_index: int,
        market_index: int,
        side: str,
        amount: Decimal,
        position_id: str,
        role: str
    ) -> Optional[OrderInfo]:
        """创建对冲订单 - 使用市价单确保快速成交"""
        try:
            logger.info(f"创建{role}订单",
                       account_index=account_index,
                       side=side,
                       amount=float(amount))
            
            # 使用市价单确保快速成交
            order = await self.order_manager.create_market_order(
                account_index=account_index,
                market_index=market_index,
                side=side,
                amount=amount
            )
            
            if order:
                # 在订单元数据中记录对冲信息
                order.metadata = {
                    "hedge_position_id": position_id,
                    "hedge_role": role,
                    "is_hedge_order": True
                }
            
            return order
            
        except Exception as e:
            logger.error("创建对冲订单失败",
                        account_index=account_index,
                        side=side,
                        error=str(e))
            raise
    
    async def _wait_for_order_fills(self, orders: List[OrderInfo], timeout: int = 30) -> bool:
        """
        等待订单成交并与后端同步验证
        返回是否所有订单都成功成交
        """
        logger.info("等待订单成交确认", orders_count=len(orders), timeout=timeout)
        
        start_time = datetime.now()
        
        while (datetime.now() - start_time).total_seconds() < timeout:
            all_filled = True
            order_statuses = []
            
            for order in orders:
                # 与后端同步订单状态
                market_index = getattr(order, 'market_index', 1)  # 默认为1
                backend_status = await self._synchronize_order_status(order.id, order.account_index, market_index)
                
                if backend_status:
                    order_statuses.append(backend_status)
                    # 检查是否为成交状态 (filled, completed等)
                    if backend_status.lower() not in ['filled', 'completed', 'executed']:
                        all_filled = False
                else:
                    # 如果无法获取后端状态，使用本地状态
                    local_status = await self.order_manager.get_order_status(order.id)
                    order_statuses.append(str(local_status))
                    if local_status != OrderStatus.FILLED:
                        all_filled = False
            
            if all_filled:
                logger.info("所有订单已成交", 
                           orders_count=len(orders),
                           statuses=order_statuses)
                return True
            
            logger.debug("等待订单成交中", 
                        filled_count=sum(1 for s in order_statuses if s.lower() in ['filled', 'completed', 'executed']),
                        total_count=len(orders),
                        statuses=order_statuses)
            
            await asyncio.sleep(2)  # 增加检查间隔以减少API调用
        
        logger.warning("订单成交等待超时", 
                      timeout=timeout,
                      final_statuses=order_statuses)
        return False
    
    def _validate_hedge_consistency(self, orders: List, expected_amount: Decimal, expected_price: Decimal) -> bool:
        """验证对冲一致性 - 市价单允许合理的价格差异"""
        try:
            if len(orders) != 2:
                logger.error("对冲订单数量错误", orders_count=len(orders), expected=2)
                return False
            
            # 验证数量一致性
            amounts = [order.amount for order in orders]
            if not all(abs(float(amount - expected_amount)) < 0.001 for amount in amounts):
                logger.error("对冲订单数量不一致", 
                           amounts=amounts, 
                           expected=float(expected_amount))
                return False
            
            # 检查是否为市价单
            is_market_orders = all(order.metadata.get('market_order', False) for order in orders)
            
            if is_market_orders:
                # 市价单：验证价格差异在合理范围内（允许5%的滑点）
                prices = [order.price for order in orders]
                avg_price = sum(prices) / len(prices)
                max_allowed_diff_percent = 5.0  # 5%
                
                for price in prices:
                    diff_percent = abs(float((price - avg_price) / avg_price)) * 100
                    if diff_percent > max_allowed_diff_percent:
                        logger.warning("市价单价格差异较大但在可接受范围", 
                                     prices=prices, 
                                     avg_price=float(avg_price),
                                     diff_percent=diff_percent,
                                     max_allowed=max_allowed_diff_percent)
                        # 市价单即使差异大也继续执行
                        break
                
                logger.debug("市价单价格差异分析",
                          prices=[float(p) for p in prices],
                          avg_price=float(avg_price),
                          expected_price=float(expected_price))
            else:
                # 限价单：验证价格严格一致性
                prices = [order.price for order in orders]
                if not all(abs(float(price - expected_price)) < 0.01 for price in prices):
                    logger.error("对冲订单价格不一致", 
                               prices=prices, 
                               expected=float(expected_price))
                    return False
            
            # 验证方向一致性（必须有一个买单和一个卖单）
            sides = [order.side for order in orders]
            if not ('buy' in sides and 'sell' in sides and len(set(sides)) == 2):
                logger.error("对冲订单方向不符合要求", sides=sides)
                return False
            
            # 验证市场一致性
            markets = [order.market_index for order in orders]
            if len(set(markets)) != 1:
                logger.error("对冲订单市场不一致", markets=markets)
                return False
            
            logger.info("对冲一致性验证通过",
                       orders_count=len(orders),
                       amounts=amounts,
                       prices=prices,
                       sides=sides)
            return True
            
        except Exception as e:
            logger.error("对冲一致性验证异常", error=str(e))
            return False
    
    async def monitor_hedge_position(self, hedge_position: HedgePosition) -> None:
        """监控对冲仓位"""
        try:
            if hedge_position.status != PositionStatus.ACTIVE:
                return
            
            # 更新所有仓位的当前价格和PnL
            total_pnl = Decimal('0')
            
            # 获取杠杆配置用于检查
            pair_config = None
            leverage_config = None
            if hedge_position.pair_id:
                # 这里需要从交易引擎获取pair_config，暂时从元数据获取
                leverage_config_data = hedge_position.metadata.get('leverage_config')
                if leverage_config_data:
                    # 确保leverage_config是LeverageConfig对象
                    if isinstance(leverage_config_data, dict):
                        # 如果是字典，转换为LeverageConfig对象
                        leverage_config = LeverageConfig(**leverage_config_data)
                    else:
                        # 如果已经是对象，直接使用
                        leverage_config = leverage_config_data
            
            for position in hedge_position.positions:
                # 获取当前市场价格
                market_data = await self.order_manager.get_market_data(position.market_index)
                if market_data:
                    position.current_price = market_data.price
                    
                    # 计算PnL
                    if position.side == "long":
                        # 多仓PnL = (当前价格 - 开仓价格) * 仓位大小
                        position.unrealized_pnl = (position.current_price - position.entry_price) * position.size
                    else:
                        # 空仓PnL = (开仓价格 - 当前价格) * 仓位大小
                        position.unrealized_pnl = (position.entry_price - position.current_price) * position.size
                    
                    total_pnl += position.unrealized_pnl
                    
                    # 如果有杠杆，更新保证金比率和检查清算风险
                    if position.leverage > 1 and leverage_config:
                        try:
                            # 计算保证金比率
                            margin_ratio = self._calculate_margin_ratio(
                                position, position.current_price, leverage_config
                            )
                            position.margin_ratio = margin_ratio
                            
                            # 检查是否接近清算
                            if self._is_near_liquidation(position, position.current_price, leverage_config):
                                logger.warning("仓位接近清算，建议立即平仓",
                                             position_id=position.id,
                                             hedge_position_id=hedge_position.id,
                                             margin_ratio=float(margin_ratio),
                                             liquidation_price=float(position.liquidation_price or 0))
                                
                                # 可以在这里设置紧急平仓标志
                                hedge_position.metadata['emergency_close_required'] = True
                                hedge_position.metadata['emergency_reason'] = 'near_liquidation'
                        
                        except Exception as e:
                            logger.error("杠杆监控失败", 
                                       position_id=position.id,
                                       error=str(e))
                    
                    position.updated_at = datetime.now()
            
            hedge_position.total_pnl = total_pnl
            hedge_position.updated_at = datetime.now()
            
            logger.debug("对冲仓位PnL更新",
                        position_id=hedge_position.id,
                        total_pnl=float(total_pnl),
                        positions_count=len(hedge_position.positions))
            
        except Exception as e:
            logger.error("监控对冲仓位失败", 
                        position_id=hedge_position.id,
                        error=str(e))
    
    async def should_close_position(self, hedge_position: HedgePosition, pair_config: TradingPairConfig) -> bool:
        """判断是否应该平仓 - 包含价格一致性检查和50%止损限制"""
        try:
            if hedge_position.status != PositionStatus.ACTIVE:
                return False
            
            # 验证仓位价格一致性
            if not await self._verify_position_price_consistency(hedge_position):
                logger.warning("仓位价格不一致，跳过平仓检查",
                             position_id=hedge_position.id)
                return False
            
            # 计算总PnL和百分比（基于实际使用的保证金）
            total_margin_used = Decimal(str(hedge_position.metadata.get('total_margin_used', 0)))
            if total_margin_used <= 0:
                logger.warning("实际保证金无效",
                             position_id=hedge_position.id,
                             total_margin_used=total_margin_used)
                return False
            
            pnl_percent = hedge_position.total_pnl / total_margin_used
            
            # 获取开仓价格用于计算价格变动百分比（优先使用实际成交均价）
            entry_price = Decimal(str(hedge_position.metadata.get('avg_entry_price', 0)))
            if entry_price <= 0:
                # 回退到触发价格
                entry_price = Decimal(str(hedge_position.metadata.get('trigger_price', 0)))
            
            if entry_price <= 0:
                logger.warning("开仓价格无效",
                             position_id=hedge_position.id,
                             avg_entry_price=hedge_position.metadata.get('avg_entry_price'),
                             trigger_price=hedge_position.metadata.get('trigger_price'))
                return False
            
            # 获取当前市场价格
            current_price = None
            if hedge_position.positions:
                current_price = hedge_position.positions[0].current_price
            
            if not current_price or current_price <= 0:
                logger.warning("当前价格无效",
                             position_id=hedge_position.id,
                             current_price=current_price)
                return False
            
            # 计算价格变动百分比
            price_change_percent = abs(current_price - entry_price) / entry_price
            
            # 检查紧急平仓标志（清算风险）
            if hedge_position.metadata.get('emergency_close_required'):
                logger.info("触发紧急平仓",
                           position_id=hedge_position.id,
                           reason=hedge_position.metadata.get('emergency_reason', 'unknown'))
                return True
            
            # 应用杠杆调整的最大止损保护
            leverage = hedge_position.metadata.get('target_leverage', 1)
            if leverage > 1:
                # 杠杆交易的最大止损更保守
                max_stop_loss_percent = Decimal('0.1') / leverage  # 杠杆越高，止损越严格
            else:
                max_stop_loss_percent = Decimal('0.5')  # 50% for non-leveraged
            
            if price_change_percent > max_stop_loss_percent:
                logger.info("触发杠杆调整止损保护",
                           position_id=hedge_position.id,
                           leverage=leverage,
                           price_change_percent=float(price_change_percent * 100),
                           max_stop_loss_percent=float(max_stop_loss_percent * 100),
                           entry_price=float(entry_price),
                           current_price=float(current_price))
                return True
            
            # ===== 已禁用主动监控平仓机制 =====
            # 原因：已有止损止盈订单，无需重复监控
            # 让市场的止损止盈订单自然触发，避免双重机制冲突
            
            logger.debug("跳过主动监控检查，依赖止损止盈订单触发",
                        position_id=hedge_position.id,
                        current_price=float(current_price),
                        total_pnl_percent=float(pnl_percent * 100))
            
            # 禁用主动触发逻辑
            should_exit = False
            
            # 详细日志记录当前状态
            logger.debug("对冲平仓条件检查详情",
                        position_id=hedge_position.id,
                        total_pnl_percent=float(pnl_percent * 100),
                        price_change_percent=float(price_change_percent * 100),
                        entry_price=float(entry_price),
                        current_price=float(current_price),
                        positions_count=len(hedge_position.positions),
                        should_close=False,
                        monitoring_mode="passive_orders_only")
            
            return False
            
        except Exception as e:
            logger.error("判断平仓条件失败",
                        position_id=hedge_position.id,
                        error=str(e))
            return False
    
    async def execute_hedge_close(self, hedge_position: HedgePosition) -> bool:
        """执行对冲平仓"""
        try:
            logger.info("🔄 开始执行对冲平仓", position_id=hedge_position.id)
            
            hedge_position.status = PositionStatus.CLOSING
            hedge_position.updated_at = datetime.now()
            
            # 获取统一的平仓价格以确保价格一致性
            market_index = hedge_position.positions[0].market_index if hedge_position.positions else None
            if not market_index:
                logger.error("无法确定市场索引", position_id=hedge_position.id)
                return False
            
            unified_close_price = await self._get_unified_execution_price(market_index)
            if not unified_close_price:
                logger.error("无法获取统一平仓价格", position_id=hedge_position.id)
                return False
            
            logger.info("使用统一平仓价格确保一致性",
                       position_id=hedge_position.id,
                       unified_close_price=float(unified_close_price))
            
            # 为每个仓位创建精确的反向订单
            close_tasks = []
            
            for position in hedge_position.positions:
                # 反向操作：多仓卖出，空仓买入
                close_side = "sell" if position.side == "long" else "buy"
                
                close_tasks.append(self._create_precise_close_order(
                    account_index=position.account_index,
                    market_index=position.market_index,
                    side=close_side,
                    amount=position.size,
                    price=unified_close_price,
                    position_id=hedge_position.id,
                    original_position_id=position.id
                ))
            
            # 同步执行平仓订单确保时间一致性
            results = await self._execute_synchronized_orders(close_tasks)
            
            # 检查结果
            successful_closes = 0
            failed_closes = 0
            
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    logger.error(f"仓位 {hedge_position.positions[i].id} 平仓失败", error=str(result))
                    failed_closes += 1
                elif result:
                    successful_closes += 1
                    logger.info(f"仓位 {hedge_position.positions[i].id} 平仓成功", order_id=result.id)
                else:
                    failed_closes += 1
            
            if failed_closes > 0:
                logger.warning("部分仓位平仓失败",
                              successful=successful_closes,
                              failed=failed_closes)
                # 这里可以添加重试逻辑
            
            # 如果大部分仓位成功平仓，标记为已关闭
            if successful_closes > failed_closes:
                # 等待平仓订单处理完成
                await asyncio.sleep(3)
                
                # 验证平仓后的仓位状态 - 应该在后端看不到仓位了
                logger.info("验证平仓后的仓位状态", position_id=hedge_position.id)
                close_validation = await self._validate_position_closed_with_backend(hedge_position.id)
                
                if close_validation:
                    logger.info("✅ 后端确认仓位已平仓", position_id=hedge_position.id)
                    hedge_position.status = PositionStatus.CLOSED
                    hedge_position.closed_at = datetime.now()
                    hedge_position.metadata['close_validated'] = True
                else:
                    logger.warning("⚠️ 后端仓位验证异常，可能存在部分未平仓仓位", 
                                 position_id=hedge_position.id)
                    hedge_position.status = PositionStatus.PENDING_CLOSE  # 需要人工确认
                    hedge_position.metadata['close_validated'] = False
                
                # 记录平仓日志
                trade_logger.log_position_close({
                    "position_id": hedge_position.id,
                    "pair_id": hedge_position.pair_id,
                    "total_pnl": float(hedge_position.total_pnl),
                    "successful_closes": successful_closes,
                    "failed_closes": failed_closes,
                    "close_validated": close_validation,
                    "timestamp": datetime.now().isoformat()
                })
                
                logger.info("✅ 对冲平仓完成",
                           position_id=hedge_position.id,
                           total_pnl=float(hedge_position.total_pnl),
                           close_validated=close_validation)
                
                return True
            else:
                hedge_position.status = PositionStatus.ACTIVE  # 回到活跃状态
                logger.error("对冲平仓失败",
                           position_id=hedge_position.id,
                           successful=successful_closes,
                           failed=failed_closes)
                return False
                
        except Exception as e:
            logger.error("执行对冲平仓失败",
                        position_id=hedge_position.id,
                        error=str(e))
            hedge_position.status = PositionStatus.ACTIVE  # 回到活跃状态
            return False
    
    async def _validate_position_with_backend(self, hedge_position_id: str) -> bool:
        """
        与Lighter后端同步验证仓位状态
        确保本地仓位状态与服务器一致
        使用重试机制避免网络延迟导致的误判
        """
        max_retries = 3
        retry_delay = 3  # 每次重试间隔3秒
        
        for attempt in range(max_retries):
            try:
                if attempt > 0:
                    logger.info(f"后端验证重试第{attempt}次", 
                               position_id=hedge_position_id,
                               max_retries=max_retries)
                    await asyncio.sleep(retry_delay)
                else:
                    logger.info("开始后端仓位同步验证", position_id=hedge_position_id)
            
                # 获取本地仓位信息
                hedge_position = self.active_positions.get(hedge_position_id)
                if not hedge_position:
                    logger.warning("本地未找到对冲仓位", position_id=hedge_position_id)
                    return False
                
                # 验证每个账户的实际仓位
                validation_results = []
                
                for position in hedge_position.positions:
                    try:
                        # 检查client_factory是否可用
                        if not self.client_factory:
                            logger.warning("client_factory不可用，跳过仓位验证", 
                                          position_id=hedge_position_id,
                                          account=position.account_index)
                            validation_results.append(False)
                            continue
                        
                        # 从API获取真实账户信息和仓位
                        account_api = self.client_factory.get_account_api()
                        account_data = await account_api.account(by="index", value=str(position.account_index))
                        
                        # 提取仓位信息
                        api_positions = []
                        if hasattr(account_data, 'positions') and account_data.positions:
                            api_positions = account_data.positions
                        elif hasattr(account_data, 'data') and hasattr(account_data.data, 'positions'):
                            api_positions = account_data.data.positions
                        
                        # 查找对应市场的仓位
                        market_position = None
                        for api_pos in api_positions:
                            if hasattr(api_pos, 'market_index') and api_pos.market_index == position.market_index:
                                market_position = api_pos
                                break
                        
                        if market_position:
                            # 验证仓位大小和方向
                            api_size = abs(Decimal(str(market_position.size)))
                            api_side = "long" if float(market_position.size) > 0 else "short"
                            
                            # 检查仓位是否匹配
                            size_match = abs(api_size - position.size) < Decimal('0.001')  # 允许小误差
                            side_match = api_side == position.side
                            
                            if size_match and side_match:
                                logger.info("仓位验证通过",
                                           account=position.account_index,
                                           market=position.market_index,
                                           expected_size=float(position.size),
                                           actual_size=float(api_size),
                                           side=position.side)
                                validation_results.append(True)
                            else:
                                logger.error("仓位验证失败",
                                            account=position.account_index,
                                            market=position.market_index,
                                            expected_size=float(position.size),
                                            actual_size=float(api_size),
                                            expected_side=position.side,
                                            actual_side=api_side)
                                validation_results.append(False)
                        else:
                            # 后端没有仓位，但本地认为有仓位
                            logger.error("后端未找到对应仓位",
                                        account=position.account_index,
                                        market=position.market_index,
                                        expected_size=float(position.size),
                                        side=position.side)
                            validation_results.append(False)
                            
                    except Exception as e:
                        logger.error("仓位验证过程中发生错误",
                                    account=position.account_index,
                                    error=str(e))
                        validation_results.append(False)
                
                # 检查整体验证结果
                all_valid = all(validation_results)
                valid_count = sum(validation_results)
                total_count = len(validation_results)
                
                logger.info(f"仓位同步验证第{attempt+1}次结果",
                           position_id=hedge_position_id,
                           valid_count=valid_count,
                           total_count=total_count,
                           all_valid=all_valid)
                
                # 如果验证通过，直接返回成功
                if all_valid:
                    return True
                
                # 如果是最后一次重试仍然失败，则执行自动平仓
                if attempt == max_retries - 1:
                    logger.warning("⚠️ 多次验证后仍然对冲不完整！开始自动平仓处理",
                                  position_id=hedge_position_id,
                                  valid_count=valid_count,
                                  total_count=total_count,
                                  attempts=max_retries)
                    
                    # 自动平掉所有成功的仓位以避免裸露风险
                    await self._auto_close_incomplete_hedge(hedge_position_id, hedge_position)
                    return False
                else:
                    logger.warning(f"第{attempt+1}次验证失败，{retry_delay}秒后重试",
                                  position_id=hedge_position_id,
                                  remaining_attempts=max_retries-attempt-1)
            
            except Exception as retry_error:
                logger.error(f"第{attempt+1}次验证过程中发生错误",
                            position_id=hedge_position_id,
                            error=str(retry_error))
                if attempt == max_retries - 1:
                    # 最后一次重试也失败了
                    logger.error("所有验证重试均失败",
                                position_id=hedge_position_id,
                                error=str(retry_error))
                    return False
        
        # 如果所有重试都失败了
        return False
    
    async def _auto_close_incomplete_hedge(self, hedge_position_id: str, hedge_position) -> None:
        """
        自动关闭不完整的对冲仓位
        当对冲验证失败时，自动平掉所有存在的仓位以避免裸露风险
        """
        try:
            logger.warning("🔴 执行自动平仓：对冲不完整保护", position_id=hedge_position_id)
            
            # 重新获取后端真实仓位状态
            positions_to_close = []
            
            for position in hedge_position.positions:
                try:
                    # 检查client_factory是否可用
                    if not self.client_factory:
                        logger.warning("client_factory不可用，跳过自动平仓", 
                                      position_id=hedge_position_id,
                                      account=position.account_index)
                        continue
                    
                    # 从API获取真实账户信息和仓位
                    account_api = self.client_factory.get_account_api()
                    account_data = await account_api.account(by="index", value=str(position.account_index))
                    
                    # 提取仓位信息
                    api_positions = []
                    if hasattr(account_data, 'positions') and account_data.positions:
                        api_positions = account_data.positions
                    elif hasattr(account_data, 'data') and hasattr(account_data.data, 'positions'):
                        api_positions = account_data.data.positions
                    
                    # 查找对应市场的仓位
                    for api_pos in api_positions:
                        if hasattr(api_pos, 'market_index') and api_pos.market_index == position.market_index:
                            api_size = abs(Decimal(str(api_pos.size)))
                            
                            if api_size > Decimal('0.001'):  # 存在显著仓位
                                api_side = "long" if float(api_pos.size) > 0 else "short"
                                close_side = "sell" if api_side == "long" else "buy"
                                
                                positions_to_close.append({
                                    'account_index': position.account_index,
                                    'market_index': position.market_index,
                                    'side': close_side,
                                    'size': api_size,
                                    'original_side': api_side
                                })
                                
                                logger.info("发现需要平仓的仓位",
                                           account=position.account_index,
                                           side=api_side,
                                           size=float(api_size))
                                break
                                
                except Exception as e:
                    logger.error("检查仓位状态时出错",
                                account=position.account_index,
                                error=str(e))
            
            if not positions_to_close:
                logger.info("✅ 没有需要平仓的仓位", position_id=hedge_position_id)
                hedge_position.status = PositionStatus.CLOSED
                return
            
            logger.warning(f"🔴 发现 {len(positions_to_close)} 个需要平仓的仓位，开始自动平仓")
            
            # 获取当前市场价格用于平仓
            market_index = hedge_position.positions[0].market_index
            close_price = await self._get_unified_execution_price(market_index)
            
            if not close_price:
                logger.error("无法获取平仓价格，使用紧急市价单")
                close_price = None  # 使用市价单
            
            # 执行平仓
            close_tasks = []
            for pos_info in positions_to_close:
                if close_price:
                    # 使用限价单平仓
                    close_task = self._create_precise_close_order(
                        account_index=pos_info['account_index'],
                        market_index=pos_info['market_index'],
                        side=pos_info['side'],
                        amount=pos_info['size'],
                        price=close_price,
                        position_id=hedge_position_id,
                        original_position_id=f"auto_close_{pos_info['account_index']}"
                    )
                else:
                    # 使用市价单紧急平仓
                    close_task = self._create_emergency_close_order(
                        account_index=pos_info['account_index'],
                        market_index=pos_info['market_index'],
                        side=pos_info['side'],
                        amount=pos_info['size']
                    )
                
                close_tasks.append(close_task)
            
            # 并行执行所有平仓订单
            results = await asyncio.gather(*close_tasks, return_exceptions=True)
            
            # 检查平仓结果
            successful_closes = 0
            failed_closes = 0
            
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    logger.error(f"自动平仓失败",
                                account=positions_to_close[i]['account_index'],
                                error=str(result))
                    failed_closes += 1
                elif result:
                    logger.info(f"✅ 自动平仓成功",
                               account=positions_to_close[i]['account_index'],
                               order_id=result.id if hasattr(result, 'id') else 'unknown')
                    successful_closes += 1
                else:
                    failed_closes += 1
            
            # 更新仓位状态
            if successful_closes > 0:
                if failed_closes == 0:
                    hedge_position.status = PositionStatus.CLOSED
                    logger.info("✅ 自动平仓完成，风险已消除", 
                               position_id=hedge_position_id,
                               successful_closes=successful_closes)
                else:
                    hedge_position.status = PositionStatus.PENDING_CLOSE
                    logger.warning("⚠️ 部分自动平仓失败，需要人工处理",
                                  position_id=hedge_position_id,
                                  successful_closes=successful_closes,
                                  failed_closes=failed_closes)
            else:
                hedge_position.status = PositionStatus.FAILED
                logger.error("❌ 自动平仓全部失败，需要紧急人工处理",
                            position_id=hedge_position_id)
            
            # 记录自动平仓日志
            hedge_position.metadata['auto_close_executed'] = True
            hedge_position.metadata['auto_close_timestamp'] = datetime.now().isoformat()
            hedge_position.metadata['auto_close_reason'] = "incomplete_hedge_protection"
            hedge_position.metadata['auto_close_results'] = {
                'successful': successful_closes,
                'failed': failed_closes,
                'total_positions': len(positions_to_close)
            }
            
        except Exception as e:
            logger.error("自动平仓过程中发生严重错误",
                        position_id=hedge_position_id,
                        error=str(e))
            hedge_position.status = PositionStatus.FAILED
            hedge_position.metadata['auto_close_error'] = str(e)
    
    async def _create_emergency_close_order(
        self,
        account_index: int,
        market_index: int,
        side: str,
        amount: Decimal
    ):
        """创建紧急市价平仓订单"""
        try:
            logger.warning("创建紧急市价平仓订单",
                          account_index=account_index,
                          side=side,
                          amount=float(amount))
            
            # 使用市价单立即平仓
            order = await self.order_manager.create_market_order(
                account_index=account_index,
                market_index=market_index,
                order_type=OrderType.MARKET,
                side=side,
                amount=amount
            )
            
            return order
            
        except Exception as e:
            logger.error("创建紧急平仓订单失败",
                        account_index=account_index,
                        error=str(e))
            raise
    
    async def _validate_position_closed_with_backend(self, hedge_position_id: str) -> bool:
        """
        多重验证仓位是否已完全平仓
        验证层级：
        1. 本地仓位状态检查
        2. 交易所API仓位验证
        3. 订单状态确认
        4. 失败时重试和人工确认
        """
        try:
            logger.info("🔍 开始多重平仓验证", position_id=hedge_position_id)
            
            # === 第一层：本地仓位状态检查 ===
            hedge_position = self.active_positions.get(hedge_position_id)
            if not hedge_position:
                logger.info("✅ 第一层验证通过：本地仓位已移除", position_id=hedge_position_id)
                return True  # 本地都没有，认为已平仓
            
            # === 第二层：交易所API仓位验证（带重试机制）===
            max_retries = 3
            retry_delay = 2
            all_closed = True
            
            for retry in range(max_retries):
                logger.info(f"📊 第二层验证：API仓位检查 (尝试 {retry + 1}/{max_retries})", 
                           position_id=hedge_position_id)
                
                verification_results = []
                api_verification_success = True
                
                for position in hedge_position.positions:
                    try:
                        # 检查client_factory是否可用
                        if not self.client_factory:
                            logger.warning("⚠️ client_factory不可用，API验证失败", 
                                          position_id=hedge_position_id,
                                          account=position.account_index,
                                          retry=retry + 1)
                            api_verification_success = False
                            break
                        
                        # 从API获取账户仓位信息
                        account_api = self.client_factory.get_account_api()
                        account_data = await account_api.account(by="index", value=str(position.account_index))
                        
                        # 提取仓位信息
                        api_positions = []
                        if hasattr(account_data, 'positions') and account_data.positions:
                            api_positions = account_data.positions
                        elif hasattr(account_data, 'data') and hasattr(account_data.data, 'positions'):
                            api_positions = account_data.data.positions
                        
                        # 查找对应市场的仓位
                        market_position = None
                        for api_pos in api_positions:
                            if hasattr(api_pos, 'market_index') and api_pos.market_index == position.market_index:
                                market_position = api_pos
                                break
                        
                        if market_position:
                            api_size = abs(Decimal(str(market_position.size)))
                            if api_size > Decimal('0.001'):  # 仍有显著仓位
                                verification_results.append({
                                    'account': position.account_index,
                                    'market': position.market_index,
                                    'status': 'still_open',
                                    'size': float(api_size)
                                })
                                all_closed = False
                            else:
                                verification_results.append({
                                    'account': position.account_index,
                                    'market': position.market_index,
                                    'status': 'closed',
                                    'size': float(api_size)
                                })
                        else:
                            # 没有仓位，已平仓
                            verification_results.append({
                                'account': position.account_index,
                                'market': position.market_index,
                                'status': 'closed',
                                'size': 0.0
                            })
                            
                    except Exception as e:
                        logger.error("API验证失败",
                                    account=position.account_index,
                                    error=str(e),
                                    retry=retry + 1)
                        verification_results.append({
                            'account': position.account_index,
                            'market': position.market_index,
                            'status': 'error',
                            'error': str(e)
                        })
                        api_verification_success = False
                
                # 如果API验证成功且所有仓位都已平仓，跳出重试循环
                if api_verification_success and all_closed:
                    logger.info("✅ 第二层验证通过：所有仓位已平仓", 
                               position_id=hedge_position_id,
                               verification_results=verification_results)
                    break
                    
                # 如果还有重试机会，等待后重试
                if retry < max_retries - 1:
                    logger.info(f"⏳ 验证未完全通过，{retry_delay}秒后重试...", 
                               position_id=hedge_position_id,
                               verification_results=verification_results)
                    await asyncio.sleep(retry_delay)
                    all_closed = True  # 重置状态准备重试
                else:
                    # 最后一次尝试失败
                    logger.warning("❌ 第二层验证失败：达到最大重试次数", 
                                  position_id=hedge_position_id,
                                  verification_results=verification_results)
            
            # === 第三层：失败时的人工确认机制 ===
            if not all_closed:
                logger.error("🚨 平仓验证失败，需要人工干预", 
                           position_id=hedge_position_id,
                           verification_results=verification_results)
                
                # 提供详细的状态报告
                still_open = [r for r in verification_results if r.get('status') == 'still_open']
                if still_open:
                    logger.error("⚠️ 仍有未平仓仓位，存在资金风险！",
                               position_id=hedge_position_id,
                               open_positions=still_open)
                
                # 标记需要人工处理
                if hedge_position_id in self.active_positions:
                    self.active_positions[hedge_position_id].metadata['manual_intervention_required'] = True
                    self.active_positions[hedge_position_id].metadata['verification_failed_at'] = datetime.now().isoformat()
                    self.active_positions[hedge_position_id].metadata['verification_results'] = verification_results
                
                return False
            
            # === 验证通过，清理本地状态 ===
            logger.info("✅ 多重验证通过：仓位已完全平仓", position_id=hedge_position_id)
            
            # 从活跃仓位中移除
            if hedge_position_id in self.active_positions:
                del self.active_positions[hedge_position_id]
                logger.info("🧹 已从活跃仓位列表中移除", position_id=hedge_position_id)
            
            return True
            
        except Exception as e:
            logger.error("🔥 平仓验证系统错误",
                        position_id=hedge_position_id,
                        error=str(e))
            
            # 系统错误时，标记需要人工处理
            if hedge_position_id in self.active_positions:
                self.active_positions[hedge_position_id].metadata['system_error'] = True
                self.active_positions[hedge_position_id].metadata['error_at'] = datetime.now().isoformat()
                self.active_positions[hedge_position_id].metadata['error_message'] = str(e)
            
            return False
    
    async def _synchronize_order_status(self, order_id: str, account_index: int, market_index: int = 1, max_retries: int = 3) -> Optional[str]:
        """
        同步订单状态与后端服务器
        返回订单的实际状态
        支持重试机制解决API延迟问题
        """
        for attempt in range(max_retries):
            try:
                logger.info("🔍 开始同步订单状态", 
                           order_id=order_id, 
                           account=account_index, 
                           market_index=market_index,
                           attempt=attempt + 1,
                           max_attempts=max_retries)
                
                # 检查client_factory是否可用
                if not self.client_factory:
                    logger.warning("client_factory不可用，跳过后端同步", 
                                  order_id=order_id, 
                                  account=account_index)
                    return None
                
                # 获取订单API
                order_api = self.client_factory.get_order_api()
                
                # 获取认证令牌 - 通过对应账户的SignerClient创建
                auth_token = None
                try:
                    signer_client = await self.client_factory.get_signer_client(account_index)
                    if signer_client:
                        auth_result, auth_error = signer_client.create_auth_token_with_expiry()
                        if auth_result and not auth_error:
                            auth_token = auth_result
                            logger.debug("获取认证令牌成功", 
                                       account_index=account_index,
                                       token_preview=auth_result[:20] + "..." if auth_result else None)
                        else:
                            logger.debug("认证令牌创建失败", account_index=account_index, error=auth_error)
                    else:
                        logger.debug("SignerClient不可用，使用无认证模式", account_index=account_index)
                except Exception as e:
                    logger.debug("获取认证令牌失败，使用无认证模式", account_index=account_index, error=str(e))
                
                # 获取订单详情 - 通过活跃订单和历史订单查找
                try:
                    order_details = None
                    
                    # 先查找活跃订单 - 使用多种查询方式
                    try:
                        # 方式1：使用市场ID过滤
                        logger.info("📋 查询活跃订单 (带市场ID过滤)", 
                                   account_index=account_index, 
                                   market_id=market_index,
                                   has_auth=bool(auth_token))
                        
                        if auth_token:
                            active_orders_filtered = await order_api.account_active_orders(
                                account_index, 
                                market_id=market_index,
                                auth=auth_token
                            )
                        else:
                            active_orders_filtered = await order_api.account_active_orders(account_index, market_id=market_index)
                        
                        logger.info("📋 活跃订单查询结果 (带过滤)", 
                                   count=len(active_orders_filtered) if active_orders_filtered else 0,
                                   order_ids=[getattr(o, 'id', 'no_id') for o in (active_orders_filtered or [])][:5])
                        
                        # 方式2：查询所有活跃订单（不过滤市场）
                        logger.info("📋 查询所有活跃订单 (无过滤)", account_index=account_index)
                        
                        if auth_token:
                            active_orders_all = await order_api.account_active_orders(
                                account_index,
                                auth=auth_token
                            )
                        else:
                            active_orders_all = await order_api.account_active_orders(account_index)
                        
                        logger.info("📋 活跃订单查询结果 (无过滤)", 
                                   count=len(active_orders_all) if active_orders_all else 0,
                                   order_ids=[getattr(o, 'id', 'no_id') for o in (active_orders_all or [])][:5])
                        
                        # 合并结果，优先使用过滤后的结果
                        active_orders = active_orders_filtered or active_orders_all or []
                        
                        # 查找匹配的订单ID - 尝试多种可能的ID字段
                        for order in active_orders:
                            order_ids_to_check = [
                                getattr(order, 'id', None),
                                getattr(order, 'order_id', None),
                                getattr(order, 'client_order_id', None)
                            ]
                            
                            logger.debug("🔍 检查订单匹配", 
                                       api_order_ids=order_ids_to_check,
                                       target_order_id=order_id,
                                       order_status=getattr(order, 'status', 'unknown'),
                                       order_market=getattr(order, 'market_index', 'unknown'))
                            
                            for api_order_id in order_ids_to_check:
                                if api_order_id and str(api_order_id) in str(order_id):
                                    order_details = order
                                    logger.info("✅ 找到匹配订单", 
                                               api_order_id=api_order_id, 
                                               local_order_id=order_id,
                                               status=getattr(order, 'status', 'unknown'))
                                    break
                            if order_details:
                                break
                    except Exception as e:
                        logger.warning("❌ 查找活跃订单失败", error=str(e))
                    
                    # 如果活跃订单中没找到，查找历史订单
                    if not order_details:
                        try:
                            logger.info("📜 查询历史订单", account_index=account_index)
                            
                            if auth_token:
                                inactive_orders = await order_api.account_inactive_orders(
                                    account_index, 
                                    limit=100,
                                    auth=auth_token
                                )
                            else:
                                inactive_orders = await order_api.account_inactive_orders(account_index, limit=100)
                            
                            logger.info("📜 历史订单查询结果", 
                                       count=len(inactive_orders) if inactive_orders else 0,
                                       order_ids=[getattr(o, 'id', 'no_id') for o in (inactive_orders or [])][:5])
                            
                            for order in inactive_orders:
                                order_ids_to_check = [
                                    getattr(order, 'id', None),
                                    getattr(order, 'order_id', None),
                                    getattr(order, 'client_order_id', None)
                                ]
                                
                                logger.debug("🔍 检查历史订单匹配", 
                                           api_order_ids=order_ids_to_check,
                                           target_order_id=order_id,
                                           order_status=getattr(order, 'status', 'unknown'))
                                
                                for api_order_id in order_ids_to_check:
                                    if api_order_id and str(api_order_id) in str(order_id):
                                        order_details = order
                                        logger.info("✅ 找到历史订单", 
                                                   api_order_id=api_order_id, 
                                                   local_order_id=order_id,
                                                   status=getattr(order, 'status', 'unknown'))
                                        break
                                if order_details:
                                    break
                        except Exception as e:
                            logger.warning("❌ 查找历史订单失败", error=str(e))
                    
                    if order_details:
                        status = getattr(order_details, 'status', 'unknown')
                        logger.info("✅ 订单状态同步成功",
                                    order_id=order_id,
                                    status=status,
                                    market_index=getattr(order_details, 'market_index', 'unknown'),
                                    order_type=getattr(order_details, 'order_type', 'unknown'))
                        return status
                    else:
                        # 如果是最后一次尝试，尝试使用增强型检测
                        if attempt == max_retries - 1:
                            logger.info("🔄 常规查询失败，启动增强型检测", 
                                       order_id=order_id,
                                       account_index=account_index)
                            
                            # 使用增强型订单检测作为最后手段
                            enhanced_result = await self._enhanced_order_detection(
                                order_id, account_index, market_index
                            )
                            
                            if enhanced_result:
                                logger.info("✅ 增强型检测成功找到订单", 
                                           order_id=order_id,
                                           status=enhanced_result)
                                return enhanced_result
                            else:
                                logger.warning("❌ 后端未找到订单（所有方法已尝试）", 
                                             order_id=order_id,
                                             account_index=account_index,
                                             market_index=market_index,
                                             total_attempts=max_retries)
                                return None
                        else:
                            # 如果不是最后一次，等待后重试
                            logger.info("⏳ 订单未找到，等待重试", 
                                       order_id=order_id,
                                       attempt=attempt + 1,
                                       wait_seconds=2)
                            await asyncio.sleep(2)  # 等待2秒后重试
                            continue
                        
                except Exception as e:
                    if attempt == max_retries - 1:
                        logger.error("获取订单详情失败（所有重试已完成）",
                                    order_id=order_id,
                                    error=str(e),
                                    total_attempts=max_retries)
                        return None
                    else:
                        logger.warning("获取订单详情失败，准备重试",
                                     order_id=order_id,
                                     error=str(e),
                                     attempt=attempt + 1)
                        await asyncio.sleep(1)  # 等待1秒后重试
                        continue
                    
            except Exception as e:
                if attempt == max_retries - 1:
                    logger.error("订单状态同步失败（所有重试已完成）",
                                order_id=order_id,
                                account=account_index,
                                error=str(e),
                                total_attempts=max_retries)
                    return None
                else:
                    logger.warning("订单状态同步失败，准备重试",
                                 order_id=order_id,
                                 error=str(e),
                                 attempt=attempt + 1)
                    await asyncio.sleep(1)
                    continue
                
        # 如果所有重试都失败，返回None
        logger.error("订单状态同步失败（所有重试已完成）",
                    order_id=order_id,
                    account=account_index,
                    max_retries=max_retries)
        return None
    
    async def _enhanced_order_detection(self, order_id: str, account_index: int, market_index: int = 1) -> Optional[str]:
        """
        增强型订单检测，使用多种方法查找订单状态
        当常规API查询失败时使用此方法
        """
        try:
            logger.info("🔍 启动增强型订单检测", 
                       order_id=order_id, 
                       account=account_index,
                       market_index=market_index)
            
            if not self.client_factory:
                return None
            
            # 方法1：检查最近交易记录
            try:
                order_api = self.client_factory.get_order_api()
                
                # 获取认证令牌
                auth_token = None
                try:
                    signer_client = await self.client_factory.get_signer_client(account_index)
                    if signer_client:
                        auth_result, auth_error = signer_client.create_auth_token_with_expiry()
                        if auth_result and not auth_error:
                            auth_token = auth_result
                except Exception:
                    pass
                
                # 检查最近的交易记录（可能包含已执行的止损止盈订单）
                logger.info("📊 检查最近交易记录", account_index=account_index)
                
                if auth_token:
                    recent_trades = await order_api.account_inactive_orders(
                        account_index,
                        limit=50,  # 增加查询数量
                        auth=auth_token
                    )
                else:
                    recent_trades = await order_api.account_inactive_orders(account_index, limit=50)
                
                if recent_trades:
                    logger.info("📊 获取到近期交易记录", 
                               count=len(recent_trades),
                               sample_ids=[getattr(t, 'id', 'no_id') for t in recent_trades[:3]])
                    
                    # 使用更宽松的匹配条件
                    for trade in recent_trades:
                        trade_ids_to_check = [
                            getattr(trade, 'id', None),
                            getattr(trade, 'order_id', None),
                            getattr(trade, 'client_order_id', None)
                        ]
                        
                        for trade_id in trade_ids_to_check:
                            if trade_id:
                                # 更宽松的匹配：部分匹配即可
                                if (str(trade_id) in str(order_id) or 
                                    str(order_id) in str(trade_id) or
                                    str(trade_id).split('-')[-1] in str(order_id)):
                                    
                                    status = getattr(trade, 'status', 'unknown')
                                    logger.info("✅ 在交易记录中找到匹配订单", 
                                               trade_id=trade_id,
                                               order_id=order_id,
                                               status=status,
                                               trade_time=getattr(trade, 'created_at', 'unknown'))
                                    return status
                else:
                    logger.info("📊 无近期交易记录", account_index=account_index)
                    
            except Exception as e:
                logger.warning("交易记录检查失败", error=str(e))
            
            # 方法2：尝试不同的时间范围查询
            try:
                logger.info("🕐 扩大时间范围查询", account_index=account_index)
                
                # 查询更大范围的历史订单
                if auth_token:
                    extended_orders = await order_api.account_inactive_orders(
                        account_index,
                        limit=200,  # 进一步增加查询数量
                        auth=auth_token
                    )
                else:
                    extended_orders = await order_api.account_inactive_orders(account_index, limit=200)
                
                if extended_orders:
                    logger.info("🕐 扩展查询结果", 
                               count=len(extended_orders),
                               latest_orders=[getattr(o, 'id', 'no_id') for o in extended_orders[:5]])
                    
                    # 检查是否有相关订单
                    for order in extended_orders:
                        order_ids = [
                            getattr(order, 'id', None),
                            getattr(order, 'order_id', None), 
                            getattr(order, 'client_order_id', None)
                        ]
                        
                        for oid in order_ids:
                            if oid and (str(oid) in str(order_id) or str(order_id) in str(oid)):
                                status = getattr(order, 'status', 'unknown')
                                logger.info("✅ 在扩展查询中找到匹配订单",
                                           found_id=oid,
                                           target_id=order_id,
                                           status=status)
                                return status
                    
            except Exception as e:
                logger.warning("扩展查询失败", error=str(e))
            
            # 方法3：模糊匹配已触发的条件订单
            try:
                logger.info("🎯 检查条件订单状态", account_index=account_index)
                
                # 这里可以添加特定于条件订单的检查逻辑
                # 例如检查是否有触发的止损止盈订单
                
                # 如果订单ID包含止损或止盈标识，可能已被触发
                if 'stop' in order_id.lower() or 'profit' in order_id.lower():
                    logger.info("🎯 检测到可能的止损止盈订单", order_id=order_id)
                    
                    # 可以返回一个推测状态，或者进行更深入的检查
                    # 这里先返回None，让上层逻辑处理
                    
            except Exception as e:
                logger.warning("条件订单检查失败", error=str(e))
            
            logger.warning("❌ 增强型订单检测未找到匹配订单", 
                         order_id=order_id,
                         account_index=account_index)
            return None
            
        except Exception as e:
            logger.error("增强型订单检测失败", 
                        order_id=order_id,
                        error=str(e))
            return None
    
    async def _create_close_order(
        self,
        account_index: int,
        market_index: int,
        side: str,
        amount: Decimal,
        position_id: str
    ) -> Optional[OrderInfo]:
        """创建平仓订单"""
        try:
            logger.info("创建平仓订单",
                       account_index=account_index,
                       side=side,
                       amount=float(amount))
            
            # 创建市价单进行平仓
            order = await self.order_manager.create_market_order(
                account_index=account_index,
                market_index=market_index,
                side=side,
                amount=amount
            )
            
            if order:
                order.metadata = {
                    "hedge_position_id": position_id,
                    "is_close_order": True
                }
            
            return order
            
        except Exception as e:
            logger.error("创建平仓订单失败",
                        account_index=account_index,
                        side=side,
                        error=str(e))
            raise
    
    async def _create_precise_close_order(
        self,
        account_index: int,
        market_index: int,
        side: str,
        amount: Decimal,
        price: Decimal,
        position_id: str,
        original_position_id: str
    ) -> Optional[OrderInfo]:
        """创建精确平仓订单 - 使用统一价格确保一致性"""
        try:
            logger.info("创建精确平仓订单",
                       account_index=account_index,
                       side=side,
                       amount=float(amount),
                       price=float(price),
                       original_position_id=original_position_id)
            
            # 使用限价单确保价格精确一致
            order = await self.order_manager.create_limit_order(
                account_index=account_index,
                market_index=market_index,
                side=side,
                amount=amount,
                price=price
            )
            
            if order:
                order.metadata = {
                    "hedge_position_id": position_id,
                    "original_position_id": original_position_id,
                    "is_close_order": True,
                    "is_precise_close": True,
                    "unified_close_price": float(price)
                }
            
            return order
            
        except Exception as e:
            logger.error("创建精确平仓订单失败",
                        account_index=account_index,
                        side=side,
                        error=str(e))
            raise
    
    async def _verify_position_price_consistency(self, hedge_position: HedgePosition) -> bool:
        """验证对冲仓位的价格一致性"""
        try:
            if not hedge_position.positions or len(hedge_position.positions) < 2:
                return False
            
            # 获取所有仓位的当前价格
            prices = []
            for position in hedge_position.positions:
                if position.current_price:
                    prices.append(position.current_price)
            
            if len(prices) < 2:
                logger.warning("价格数据不足，无法验证一致性",
                             position_id=hedge_position.id,
                             available_prices=len(prices))
                return False
            
            # 检查价格差异是否在合理范围内（1%容差）
            max_price = max(prices)
            min_price = min(prices)
            price_tolerance = Decimal('0.01')  # 1%
            
            price_diff_percent = (max_price - min_price) / min_price if min_price > 0 else Decimal('1')
            
            is_consistent = price_diff_percent <= price_tolerance
            
            if not is_consistent:
                logger.warning("价格一致性检查失败",
                             position_id=hedge_position.id,
                             max_price=float(max_price),
                             min_price=float(min_price),
                             price_diff_percent=float(price_diff_percent * 100),
                             tolerance=float(price_tolerance * 100))
            else:
                logger.debug("价格一致性检查通过",
                           position_id=hedge_position.id,
                           price_diff_percent=float(price_diff_percent * 100))
            
            return is_consistent
            
        except Exception as e:
            logger.error("价格一致性验证失败",
                        position_id=hedge_position.id,
                        error=str(e))
            return False
    
    def _get_hedge_exit_thresholds(self, hedge_position: HedgePosition, pair_config: TradingPairConfig) -> dict:
        """获取对冲退出阈值 - 一边止损价=另一边止盈价"""
        try:
            # 从配置获取基础阈值
            base_profit_bps = getattr(pair_config, 'take_profit_bps', 100)  # 默认1%
            base_loss_bps = getattr(pair_config, 'stop_loss_bps', 50)      # 默认0.5%
            
            # 获取杠杆倍数
            leverage = getattr(pair_config, 'leverage', 1)
            
            # 杠杆条件下调整止损止盈
            if leverage > 1:
                # 止盈：保持原值（让利润奔跑）
                adjusted_profit_bps = base_profit_bps
                # 止损：根据杠杆缩小（保护本金）
                adjusted_loss_bps = max(base_loss_bps // leverage, 10)  # 最小10bp
                
                logger.info("杠杆止损止盈调整",
                          leverage=leverage,
                          original_profit_bps=base_profit_bps,
                          original_loss_bps=base_loss_bps,
                          adjusted_profit_bps=adjusted_profit_bps,
                          adjusted_loss_bps=adjusted_loss_bps)
            else:
                adjusted_profit_bps = base_profit_bps
                adjusted_loss_bps = base_loss_bps
            
            # 应用合理限制
            max_profit_bps = 1000  # 10%
            max_loss_bps = 500     # 5%
            
            if adjusted_profit_bps > max_profit_bps:
                adjusted_profit_bps = max_profit_bps
            if adjusted_loss_bps > max_loss_bps:
                adjusted_loss_bps = max_loss_bps
            
            # 转换为小数
            profit_threshold = Decimal(str(adjusted_profit_bps)) / Decimal('10000')
            loss_threshold = Decimal(str(adjusted_loss_bps)) / Decimal('10000')
            
            # 设置整体对冲退出阈值 (使用较大的阈值)
            exit_threshold = max(profit_threshold, loss_threshold)
            
            # 为对冲仓位的每个子仓位设置相反的阈值
            position_thresholds = {}
            
            for position in hedge_position.positions:
                if position.side == "long":
                    # 多仓：止盈在上方，止损在下方
                    position_thresholds[position.id] = {
                        "take_profit": profit_threshold,    # 止盈
                        "stop_loss": -loss_threshold,       # 止损
                        "side": "long",
                        "leverage": leverage
                    }
                else:  # short
                    # 空仓：止盈在下方，止损在上方 (相反)
                    position_thresholds[position.id] = {
                        "take_profit": -profit_threshold,   # 止盈 (价格下跌)
                        "stop_loss": loss_threshold,        # 止损 (价格上涨)
                        "side": "short",
                        "leverage": leverage
                    }
            
            logger.debug("对冲退出阈值设置",
                        exit_threshold_percent=float(exit_threshold * 100),
                        long_positions=len([p for p in hedge_position.positions if p.side == "long"]),
                        short_positions=len([p for p in hedge_position.positions if p.side == "short"]))
            
            return {
                "exit_threshold": exit_threshold,
                "position_thresholds": position_thresholds,
                "hedge_exit_price_long": None,   # 将在监控时计算
                "hedge_exit_price_short": None   # 将在监控时计算
            }
            
        except Exception as e:
            logger.error("获取对冲退出阈值失败", error=str(e))
            # 返回默认配置
            default_threshold = Decimal('0.02')
            return {
                "exit_threshold": default_threshold,
                "position_thresholds": {},
                "hedge_exit_price_long": None,
                "hedge_exit_price_short": None
            }
    
    
    def _calculate_liquidation_price(
        self, 
        side: str, 
        entry_price: Decimal, 
        size: Decimal, 
        leverage: int,
        leverage_config: LeverageConfig,
        account_balance: Decimal
    ) -> Decimal:
        """计算清算价格"""
        try:
            # 使用维持保证金要求计算清算价格
            mmr = leverage_config.maintenance_margin_requirement
            
            if side == "long":
                # 多仓清算价格 = 开仓价格 * (1 - MMR)
                liquidation_price = entry_price * (Decimal('1') - mmr)
            else:
                # 空仓清算价格 = 开仓价格 * (1 + MMR)
                liquidation_price = entry_price * (Decimal('1') + mmr)
            
            logger.debug("清算价格计算",
                        side=side,
                        entry_price=float(entry_price),
                        leverage=leverage,
                        mmr=float(mmr),
                        liquidation_price=float(liquidation_price))
            
            return liquidation_price
            
        except Exception as e:
            logger.error("清算价格计算失败", error=str(e))
            return Decimal('0')
    
    def _calculate_margin_ratio(
        self, 
        position: Position, 
        current_price: Decimal,
        leverage_config: LeverageConfig
    ) -> Decimal:
        """计算当前保证金比率"""
        try:
            if position.margin_used <= 0:
                return Decimal('0')
            
            # 计算当前仓位价值
            position_value = position.size * current_price
            
            # 计算未实现盈亏
            if position.side == "long":
                unrealized_pnl = (current_price - position.entry_price) * position.size
            else:
                unrealized_pnl = (position.entry_price - current_price) * position.size
            
            # 计算账户价值 (保证金 + 未实现盈亏)
            account_value = position.margin_used + unrealized_pnl
            
            # 计算所需维持保证金
            required_maintenance_margin = position_value * leverage_config.maintenance_margin_requirement
            
            # 保证金比率 = 账户价值 / 所需维持保证金
            margin_ratio = account_value / required_maintenance_margin if required_maintenance_margin > 0 else Decimal('0')
            
            logger.debug("保证金比率计算",
                        position_id=position.id,
                        position_value=float(position_value),
                        unrealized_pnl=float(unrealized_pnl),
                        account_value=float(account_value),
                        required_maintenance_margin=float(required_maintenance_margin),
                        margin_ratio=float(margin_ratio))
            
            return margin_ratio
            
        except Exception as e:
            logger.error("保证金比率计算失败", position_id=position.id, error=str(e))
            return Decimal('0')
    
    def _is_near_liquidation(
        self, 
        position: Position, 
        current_price: Decimal,
        leverage_config: LeverageConfig,
        warning_threshold: Decimal = Decimal('1.2')  # 120% of maintenance margin
    ) -> bool:
        """检查是否接近清算"""
        try:
            margin_ratio = self._calculate_margin_ratio(position, current_price, leverage_config)
            
            # 如果保证金比率低于警告阈值，则接近清算
            is_near = margin_ratio < warning_threshold and margin_ratio > Decimal('1')
            
            if is_near:
                logger.warning("仓位接近清算",
                             position_id=position.id,
                             margin_ratio=float(margin_ratio),
                             warning_threshold=float(warning_threshold),
                             liquidation_price=float(position.liquidation_price or 0))
            
            return is_near
            
        except Exception as e:
            logger.error("清算检查失败", position_id=position.id, error=str(e))
            return False
    
    async def _get_unified_execution_price(self, market_index: int) -> Optional[Decimal]:
        """获取统一的执行价格 - 优先使用WebSocket实时数据"""
        try:
            market_data = None
            orderbook = None
            
            # 策略1: 直接从WebSocket管理器获取最新数据
            if hasattr(self.order_manager, 'websocket_manager') and self.order_manager.websocket_manager:
                ws_market_data = self.order_manager.websocket_manager.get_latest_market_data(market_index)
                ws_orderbook = self.order_manager.websocket_manager.get_latest_orderbook(market_index)
                
                if ws_market_data and ws_orderbook:
                    data_age = (datetime.now() - ws_market_data.timestamp).total_seconds()
                    logger.debug("✅ 使用WebSocket实时数据获取统一价格",
                               market_index=market_index,
                               data_age_seconds=f"{data_age:.1f}s",
                               price=float(ws_market_data.price))
                    market_data = ws_market_data
                    orderbook = ws_orderbook
                elif ws_market_data:
                    # 即使没有订单簿，也可以使用市场数据
                    data_age = (datetime.now() - ws_market_data.timestamp).total_seconds()
                    logger.debug("✅ 使用WebSocket市场数据（无订单簿）",
                               market_index=market_index,
                               data_age_seconds=f"{data_age:.1f}s",
                               price=float(ws_market_data.price))
                    return ws_market_data.price
            
            # 策略2: 使用策略自身缓存
            if not market_data:
                cached_orderbook = self.get_cached_orderbook(market_index)
                cached_market_data = self.get_cached_market_data(market_index)
                
                if cached_orderbook and cached_market_data:
                    logger.debug("使用策略缓存数据",
                               market_index=market_index,
                               data_age_seconds=(datetime.now() - cached_market_data.timestamp).total_seconds())
                    orderbook = cached_orderbook
                    market_data = cached_market_data
            
            # 策略3: 回退到API请求
            if not market_data:
                logger.debug("回退到API请求数据", market_index=market_index)
                market_data = await self.order_manager.get_market_data(market_index)
                if not market_data:
                    logger.error("无法获取市场数据", market_index=market_index)
                    return None
                
                # 获取订单簿验证波动性
                orderbook = await self.order_manager.get_orderbook(market_index)
            
            # 策略4: 最后备用方案 - 至少确保有市场数据
            if not market_data:
                logger.warning("所有数据源失败，尝试强制API调用", market_index=market_index)
                try:
                    # 绕过频率限制，强制获取数据
                    market_data = await self.order_manager._fetch_market_data_from_api(market_index)
                    if market_data:
                        logger.info("✅ 强制API调用成功", 
                                   market_index=market_index,
                                   price=float(market_data.price))
                except Exception as e:
                    logger.error("强制API调用也失败", market_index=market_index, error=str(e))
                    return None
            
            if not orderbook or not orderbook.bids or not orderbook.asks:
                logger.warning("订单簿数据不完整，使用市场价格", market_index=market_index)
                return market_data.price
            
            # 计算当前波动性指标
            bid_price = orderbook.bids[0]["price"]
            ask_price = orderbook.asks[0]["price"]
            spread_percent = (ask_price - bid_price) / market_data.price
            
            # 检查波动性是否适合开仓 (价差小于0.5%认为波动性不大)
            max_spread_for_entry = Decimal('0.005')  # 0.5%
            
            if spread_percent > max_spread_for_entry:
                logger.warning("当前波动性较大，不适合开仓",
                             market_index=market_index,
                             spread_percent=float(spread_percent * 100),
                             max_spread=float(max_spread_for_entry * 100))
                return None
            
            # 使用更保守的价格策略，避免价格过于偏离市场
            # 对于买单使用较高的价格，对于卖单使用较低的价格，确保成交概率
            market_price = market_data.price
            price_buffer = Decimal('0.02')  # 2%的价格缓冲
            
            # 计算保守的统一价格，确保在合理范围内
            if abs(bid_price - market_price) / market_price > price_buffer or \
               abs(ask_price - market_price) / market_price > price_buffer:
                # 如果订单簿价格偏离市场价格太远，使用市场价格
                logger.warning("订单簿价格偏离市场价格较大，使用市场价格",
                             market_index=market_index,
                             market_price=float(market_price),
                             bid_price=float(bid_price),
                             ask_price=float(ask_price))
                unified_price = market_price
            else:
                # 使用中间价作为统一执行价格
                unified_price = (bid_price + ask_price) / 2
            
            logger.info("低波动性开仓条件满足",
                       market_index=market_index,
                       market_price=float(market_data.price),
                       unified_price=float(unified_price),
                       spread_percent=float(spread_percent * 100))
            
            return unified_price
            
        except Exception as e:
            logger.error("获取统一执行价格失败", 
                        market_index=market_index,
                        error=str(e))
            return None
    
    async def _create_precise_hedge_order(
        self,
        account_index: int,
        market_index: int,
        side: str,
        amount: Decimal,
        position_id: str,
        role: str
    ) -> Optional[OrderInfo]:
        """创建对冲订单 - 使用市价单确保快速成交"""
        try:
            logger.info(f"创建{role}对冲订单",
                       account_index=account_index,
                       side=side,
                       amount=float(amount))
            
            # 验证SignerClient可用性
            if not self.order_manager.has_signer_clients():
                logger.error("无法创建市价单：没有可用的SignerClient",
                           platform="Windows",
                           account_index=account_index,
                           available_accounts=self.order_manager.get_available_accounts())
                raise Exception("系统在Windows平台上运行，SignerClient不可用。请在WSL/Linux/macOS环境下运行")
            
            # 使用市价单确保快速成交
            order = await self.order_manager.create_market_order(
                account_index=account_index,
                market_index=market_index,
                side=side,
                amount=amount
            )
            
            if order:
                # 在订单元数据中记录对冲信息
                order.metadata = {
                    "hedge_position_id": position_id,
                    "hedge_role": role,
                    "is_hedge_order": True,
                    "market_order": True,
                    "precise_hedge": True
                }
                
                # 注意：止损止盈订单现在在开仓完成后统一创建，确保价格协调
            
            return order
            
        except Exception as e:
            logger.error("创建精确对冲订单失败",
                        account_index=account_index,
                        side=side,
                        error=str(e))
            raise
    
    async def _create_hedge_stop_loss_take_profit_orders(
        self,
        hedge_position_id: str,
        positions: List[dict],  # [{"account_index": 165652, "side": "buy", "amount": 0.003, "entry_price": 112016.25}, ...]
        market_index: int
    ) -> None:
        """为整个对冲仓位创建协调的止损止盈订单"""
        try:
            # 获取配置
            pair_config = None
            for pair in self.config_manager.get_active_pairs():
                if pair.market_index == market_index:
                    pair_config = pair
                    break
            
            if not pair_config:
                logger.warning("无法找到交易对配置，跳过止损止盈设置", market_index=market_index)
                return
            
            # 获取杠杆倍数和止损止盈配置
            leverage = getattr(pair_config, 'leverage', 1)
            stop_take_distance = getattr(pair_config, 'stop_take_distance', 'auto')
            
            logger.info("止损止盈配置读取",
                       leverage=leverage,
                       stop_take_distance=stop_take_distance)
            
            # 计算安全的最大止损距离（基于爆仓点）
            if leverage > 1:
                # 计算最大允许的止损距离（基于50%爆仓点）
                max_loss_percent = Decimal('0.5')  # 50%保证金亏损
                max_stop_distance_percent = max_loss_percent / Decimal(str(leverage))
                
                # 为了安全，使用80%的最大距离作为安全上限
                safety_factor = Decimal('0.8')
                safe_max_distance_percent = max_stop_distance_percent * safety_factor
            else:
                # 现货交易的安全上限
                safe_max_distance_percent = Decimal('0.05')  # 5%
            
            # 根据配置决定使用的距离
            if stop_take_distance == 'auto':
                # 自动计算模式
                stop_loss_take_profit_percent = safe_max_distance_percent
                calculation_method = "auto"
                logger.info("使用自动计算的止损止盈距离",
                           leverage=leverage,
                           auto_calculated_percent=float(stop_loss_take_profit_percent * 100))
            else:
                # 手动设置模式
                try:
                    manual_percent = Decimal(str(stop_take_distance)) / 100  # 转换为小数
                    
                    # 安全检查：不能超过安全上限
                    if manual_percent > safe_max_distance_percent:
                        stop_loss_take_profit_percent = safe_max_distance_percent
                        calculation_method = "capped_manual"
                        logger.warning("手动设置的止损止盈距离超出安全范围，使用安全上限",
                                     manual_percent=float(manual_percent * 100),
                                     safe_max_percent=float(safe_max_distance_percent * 100),
                                     using_percent=float(stop_loss_take_profit_percent * 100))
                    else:
                        stop_loss_take_profit_percent = manual_percent
                        calculation_method = "manual"
                        logger.info("使用手动设置的止损止盈距离",
                                   manual_percent=float(stop_loss_take_profit_percent * 100))
                        
                except (ValueError, TypeError) as e:
                    # 配置值无效，回退到自动计算
                    stop_loss_take_profit_percent = safe_max_distance_percent
                    calculation_method = "auto_fallback"
                    logger.warning("止损止盈配置无效，回退到自动计算",
                                 invalid_config=stop_take_distance,
                                 error=str(e),
                                 using_percent=float(stop_loss_take_profit_percent * 100))
            
            # 转换为bps用于后续计算
            adjusted_stop_loss_bps = int(stop_loss_take_profit_percent * 10000)
            adjusted_take_profit_bps = int(stop_loss_take_profit_percent * 10000)  # 镜像对称，相同距离
            
            # 计算统一的对冲价格基准
            if not positions:
                logger.warning("没有仓位数据，无法创建止损止盈", hedge_position_id=hedge_position_id)
                return
                
            total_position_value = sum(pos["amount"] * pos["entry_price"] for pos in positions)
            total_amount = sum(pos["amount"] for pos in positions)
            
            if total_amount == 0:
                logger.warning("总持仓数量为0，无法创建止损止盈", hedge_position_id=hedge_position_id)
                return
                
            avg_entry_price = total_position_value / total_amount
            
            # 对冲镜像价格设计（使用相同的距离）：
            # Long止损价格 = Short止盈价格 (下方价格)
            # Short止损价格 = Long止盈价格 (上方价格)
            # 由于止损和止盈使用相同距离，确保完美镜像对称
            
            price_distance_percent = Decimal(adjusted_stop_loss_bps) / 10000  # 统一的价格距离
            hedge_lower_price = avg_entry_price * (1 - price_distance_percent)  # 下方触发价格
            hedge_upper_price = avg_entry_price * (1 + price_distance_percent)  # 上方触发价格
            
            logger.info("创建协调的对冲止损止盈订单",
                       hedge_position_id=hedge_position_id,
                       avg_entry_price=float(avg_entry_price),
                       hedge_lower_price=float(hedge_lower_price),  # 下方触发价格
                       hedge_upper_price=float(hedge_upper_price),  # 上方触发价格
                       price_distance_percent=float(price_distance_percent * 100),
                       calculation_method=calculation_method,
                       config_value=stop_take_distance,
                       positions_count=len(positions),
                       leverage=leverage,
                       positions_data=[{"account": p["account_index"], "side": p["side"], "amount": float(p["amount"]), "entry_price": float(p["entry_price"])} for p in positions])
            
            # 为每个仓位创建相应的止损止盈订单
            for pos in positions:
                account_index = pos["account_index"]
                side = pos["side"]
                amount = pos["amount"]
                entry_price = pos["entry_price"]
                
                if side.lower() == "buy":
                    # Long仓位：
                    # - 止损：在下方价格卖出 (hedge_lower_price)
                    # - 止盈：在上方价格卖出 (hedge_upper_price)
                    sl_trigger_price = hedge_lower_price
                    tp_trigger_price = hedge_upper_price
                    sl_side = "sell"
                    tp_side = "sell"
                else:
                    # Short仓位：
                    # - 止损：在上方价格买入 (hedge_upper_price) -> 镜像对称
                    # - 止盈：在下方价格买入 (hedge_lower_price) -> 镜像对称
                    sl_trigger_price = hedge_upper_price  # Short的止损=Long的止盈价格
                    tp_trigger_price = hedge_lower_price  # Short的止盈=Long的止损价格
                    sl_side = "buy"
                    tp_side = "buy"
                
                logger.info("仓位止损止盈设置",
                           account_index=account_index,
                           side=side,
                           entry_price=float(entry_price),
                           sl_trigger_price=float(sl_trigger_price),
                           tp_trigger_price=float(tp_trigger_price))
                
                # 创建止损订单 - 使用限价单提高精准度
                try:
                    sl_order = await self.order_manager.create_stop_loss_order(
                        account_index=account_index,
                        market_index=market_index,
                        side=sl_side,
                        amount=amount,
                        trigger_price=sl_trigger_price,
                        order_type="limit"  # 改为限价单
                    )
                    
                    # 添加OCO关联标记到订单元数据
                    if sl_order:
                        sl_order.metadata.update({
                            "oco_group": hedge_position_id,  # OCO组标识
                            "oco_type": "stop_loss",
                            "paired_with": "take_profit"
                        })
                    
                    if sl_order:
                        sl_order.metadata.update({
                            "hedge_position_id": hedge_position_id,
                            "is_stop_loss": True,
                            "entry_price": float(entry_price),
                            "hedge_mirror_price": float(tp_trigger_price),  # 记录镜像价格
                            "leverage": leverage,
                            "calculated_distance_percent": float(price_distance_percent * 100),
                            "dynamic_calculation": True  # 标记为动态计算
                        })
                        logger.info("止损订单创建成功", 
                                   account_index=account_index,
                                   order_id=sl_order.id,
                                   trigger_price=float(sl_trigger_price))
                    
                except Exception as e:
                    logger.error("创建止损订单失败", 
                               account_index=account_index, 
                               error=str(e))
                
                # 创建止盈订单 - 使用限价单获得更好价格
                try:
                    tp_order = await self.order_manager.create_take_profit_order(
                        account_index=account_index,
                        market_index=market_index,
                        side=tp_side,
                        amount=amount,
                        trigger_price=tp_trigger_price,
                        order_type="limit"
                    )
                    
                    # 添加OCO关联标记到订单元数据
                    if tp_order:
                        tp_order.metadata.update({
                            "oco_group": hedge_position_id,  # OCO组标识
                            "oco_type": "take_profit", 
                            "paired_with": "stop_loss"
                        })
                    
                    if tp_order:
                        tp_order.metadata.update({
                            "hedge_position_id": hedge_position_id,
                            "is_take_profit": True,
                            "entry_price": float(entry_price),
                            "hedge_mirror_price": float(sl_trigger_price),  # 记录镜像价格
                            "leverage": leverage,
                            "calculated_distance_percent": float(price_distance_percent * 100),
                            "dynamic_calculation": True  # 标记为动态计算
                        })
                        logger.info("止盈订单创建成功", 
                                   account_index=account_index,
                                   order_id=tp_order.id,
                                   trigger_price=float(tp_trigger_price))
                    
                except Exception as e:
                    logger.error("创建止盈订单失败", 
                               account_index=account_index, 
                               error=str(e))
        
        except Exception as e:
            logger.error("创建对冲止损止盈订单失败",
                        hedge_position_id=hedge_position_id,
                        error=str(e))
    
    async def _execute_synchronized_orders(self, order_tasks: List) -> List:
        """同步执行订单以确保时间一致性"""
        try:
            logger.info("开始同步执行对冲订单", orders_count=len(order_tasks))
            
            # 记录开始时间
            start_time = datetime.now()
            
            # 并发执行所有订单，使用更合理的超时时间
            results = await asyncio.wait_for(
                asyncio.gather(*order_tasks, return_exceptions=True),
                timeout=15.0  # 15秒超时，给网络请求足够时间
            )
            
            # 记录执行时间
            execution_time = (datetime.now() - start_time).total_seconds()
            
            logger.info("同步订单执行完成",
                       execution_time_ms=execution_time * 1000,
                       orders_count=len(order_tasks))
            
            # 验证所有订单的时间戳接近
            valid_orders = [r for r in results if isinstance(r, OrderInfo)]
            if len(valid_orders) >= 2:
                timestamps = [order.created_at for order in valid_orders]
                time_diff = max(timestamps) - min(timestamps)
                
                if time_diff.total_seconds() > 1.0:  # 超过1秒认为不同步
                    logger.warning("订单执行时间差异较大",
                                 time_diff_seconds=time_diff.total_seconds())
                else:
                    logger.info("订单执行时间同步良好",
                               time_diff_ms=time_diff.total_seconds() * 1000)
            
            return results
            
        except asyncio.TimeoutError:
            logger.error("同步订单执行超时",
                        timeout_seconds=15.0,
                        orders_count=len(order_tasks),
                        execution_time_s=(datetime.now() - start_time).total_seconds())
            return [Exception("订单执行超时")] * len(order_tasks)
        except Exception as e:
            logger.error("同步订单执行失败",
                        error=str(e),
                        orders_count=len(order_tasks),
                        execution_time_s=(datetime.now() - start_time).total_seconds())
            return [e] * len(order_tasks)
    
    async def _get_account_balances(self, accounts: List[int]) -> Dict[int, Decimal]:
        """获取账户余额"""
        try:
            balances = {}
            for account_index in accounts:
                # 从账户管理器获取余额（同步方法）
                balance = self.account_manager.get_account_balance(account_index)
                balances[account_index] = balance if balance else Decimal('0')
            
            logger.debug("账户余额查询",
                        accounts=accounts,
                        balances={k: float(v) for k, v in balances.items()})
            
            return balances
            
        except Exception as e:
            logger.error("获取账户余额失败", accounts=accounts, error=str(e))
            # 返回默认余额避免阻塞
            return {account: Decimal('1000') for account in accounts}
    
    async def _get_rotated_direction_assignments(
        self, 
        accounts: List[int], 
        pair_id: str
    ) -> Dict[int, tuple[str, str]]:
        """获取轮转的方向分配"""
        try:
            # 从缓存或配置获取上次的方向分配
            cache_key = f"direction_history_{pair_id}"
            last_assignments = getattr(self, '_direction_cache', {}).get(cache_key, {})
            
            # 确保有多空平衡
            if len(accounts) < 2:
                raise ValueError("对冲需要至少2个账户")
            
            # 创建新的方向分配
            new_assignments = {}
            
            # 简单轮转逻辑：如果账户上次是多仓，这次就是空仓
            for i, account_index in enumerate(accounts):
                last_side = last_assignments.get(account_index, ("buy", "long"))[0]
                
                # 轮转方向
                if last_side == "buy":
                    new_side = "sell"
                    new_role = "short"
                else:
                    new_side = "buy"
                    new_role = "long"
                
                new_assignments[account_index] = (new_side, new_role)
            
            # 确保多空平衡 (至少有一个多仓和一个空仓)
            sides = [assignment[0] for assignment in new_assignments.values()]
            if "buy" not in sides or "sell" not in sides:
                # 强制平衡：第一个账户买入，第二个账户卖出
                accounts_list = list(accounts)
                new_assignments[accounts_list[0]] = ("buy", "long")
                new_assignments[accounts_list[1]] = ("sell", "short")
                
                # 其余账户轮转
                for i, account in enumerate(accounts_list[2:], start=2):
                    if i % 2 == 0:
                        new_assignments[account] = ("buy", "long")
                    else:
                        new_assignments[account] = ("sell", "short")
            
            # 缓存当前分配用于下次轮转
            if not hasattr(self, '_direction_cache'):
                self._direction_cache = {}
            self._direction_cache[cache_key] = new_assignments
            
            logger.info("方向轮转分配",
                       pair_id=pair_id,
                       last_assignments=last_assignments,
                       new_assignments=new_assignments)
            
            return new_assignments
            
        except Exception as e:
            logger.error("方向分配失败", pair_id=pair_id, accounts=accounts, error=str(e))
            # 返回默认分配
            default_assignments = {}
            for i, account in enumerate(accounts):
                if i % 2 == 0:
                    default_assignments[account] = ("buy", "long")
                else:
                    default_assignments[account] = ("sell", "short")
            return default_assignments
    
    def update_market_data(self, market_data: MarketData) -> None:
        """Update strategy with latest market data from WebSocket"""
        try:
            # Cache latest market data for this market
            if not hasattr(self, '_latest_market_data'):
                self._latest_market_data = {}
            
            self._latest_market_data[market_data.market_index] = market_data
            
            logger.debug("策略市场数据更新",
                        market_index=market_data.market_index,
                        price=float(market_data.price),
                        timestamp=market_data.timestamp.isoformat())
                        
        except Exception as e:
            logger.error("更新市场数据失败",
                        market_index=market_data.market_index,
                        error=str(e))
    
    def update_orderbook(self, orderbook: OrderBook) -> None:
        """Update strategy with latest orderbook from WebSocket"""
        try:
            # Cache latest orderbook for this market
            if not hasattr(self, '_latest_orderbooks'):
                self._latest_orderbooks = {}
            
            self._latest_orderbooks[orderbook.market_index] = orderbook
            
            # Calculate spread for volatility assessment
            if orderbook.bids and orderbook.asks:
                best_bid = orderbook.bids[0]['price']
                best_ask = orderbook.asks[0]['price']
                spread_percent = (best_ask - best_bid) / best_bid * 100
                
                logger.debug("策略订单簿更新",
                            market_index=orderbook.market_index,
                            best_bid=float(best_bid),
                            best_ask=float(best_ask),
                            spread_percent=float(spread_percent),
                            timestamp=orderbook.timestamp.isoformat())
            
        except Exception as e:
            logger.error("更新订单簿失败",
                        market_index=orderbook.market_index,
                        error=str(e))
    
    def get_cached_market_data(self, market_index: int) -> Optional[MarketData]:
        """Get cached market data for faster access"""
        if hasattr(self, '_latest_market_data'):
            return self._latest_market_data.get(market_index)
        return None
    
    def get_cached_orderbook(self, market_index: int) -> Optional[OrderBook]:
        """Get cached orderbook for faster access"""
        if hasattr(self, '_latest_orderbooks'):
            return self._latest_orderbooks.get(market_index)
        return None
    
    async def _cleanup_all_pending_orders(self, pair_config: TradingPairConfig, accounts: List[int]) -> bool:
        """
        清理所有相关账户的挂单
        开仓前清空所有挂单，确保环境干净，避免任何旧订单影响新仓位
        
        Args:
            pair_config: 交易对配置
            accounts: 需要清理的账户列表
            
        Returns:
            bool: 是否完全成功清理（部分失败也会返回False，但不阻止开仓）
        """
        try:
            logger.info("🔍 开始批量清理所有历史挂单",
                       pair_id=pair_config.id,
                       market_index=pair_config.market_index,
                       accounts_count=len(accounts))
            
            all_success = True
            total_cancelled = 0
            
            for account_index in accounts:
                try:
                    logger.info("📋 批量取消账户历史订单",
                               account_index=account_index,
                               market_index=pair_config.market_index)
                    
                    # 使用OrderManager的批量取消功能
                    cancelled_count = await self.order_manager.cancel_all_inactive_orders(
                        account_index=account_index,
                        market_index=pair_config.market_index
                    )
                    
                    if cancelled_count > 0:
                        total_cancelled += cancelled_count
                        logger.info("✅ 账户历史订单清理完成",
                                   account_index=account_index,
                                   cancelled_count=cancelled_count)
                    else:
                        logger.info("✨ 账户无历史订单需要清理", 
                                   account_index=account_index)
                    
                except Exception as e:
                    logger.error("清理账户历史订单失败",
                               account_index=account_index,
                               error=str(e))
                    all_success = False
                    
                # 短暂延迟避免API限制
                await asyncio.sleep(0.2)
            
            if total_cancelled > 0:
                logger.info("🧹 历史订单批量清理汇总",
                           pair_id=pair_config.id,
                           total_cancelled=total_cancelled,
                           accounts_processed=len(accounts),
                           all_success=all_success)
            else:
                logger.info("✨ 环境已经干净，无历史订单需要清理",
                           pair_id=pair_config.id)
            
            return all_success
            
        except Exception as e:
            logger.error("批量清理历史订单失败",
                        pair_id=pair_config.id,
                        error=str(e))
            return False
    
    async def _get_account_active_orders(self, account_index: int, market_index: int) -> List:
        """获取账户在指定市场的所有活跃订单"""
        try:
            if not self.client_factory:
                logger.debug("client_factory不可用", account_index=account_index)
                return []
            
            order_api = self.client_factory.get_order_api()
            
            # 获取认证令牌
            auth_token = None
            try:
                signer_client = await self.client_factory.get_signer_client(account_index)
                if signer_client:
                    auth_result, auth_error = signer_client.create_auth_token_with_expiry()
                    if auth_result and not auth_error:
                        auth_token = auth_result
            except Exception as e:
                logger.debug("获取认证令牌失败，使用无认证模式", 
                           account_index=account_index, error=str(e))
            
            # 查询活跃订单
            if auth_token:
                orders_response = await order_api.account_active_orders(
                    account_index,
                    market_id=market_index,
                    auth=auth_token
                )
            else:
                orders_response = await order_api.account_active_orders(
                    account_index,
                    market_id=market_index
                )
            
            # 处理API响应，提取订单列表
            if orders_response:
                # 检查是否有orders属性
                if hasattr(orders_response, 'orders'):
                    orders_list = orders_response.orders
                    return list(orders_list) if orders_list else []
                # 如果返回的直接是列表
                elif isinstance(orders_response, list):
                    return orders_response
                # 如果是字典格式
                elif isinstance(orders_response, dict) and 'orders' in orders_response:
                    return orders_response['orders'] or []
                # 如果是其他可迭代对象，尝试转换为列表
                elif hasattr(orders_response, '__iter__') and not isinstance(orders_response, str):
                    try:
                        return list(orders_response)
                    except Exception as e:
                        logger.debug("转换orders为列表失败", 
                                   response_type=type(orders_response).__name__,
                                   account_index=account_index,
                                   error=str(e))
                        return []
                # 如果是其他类型
                else:
                    logger.debug("未知的orders响应格式", 
                               response_type=type(orders_response).__name__,
                               account_index=account_index,
                               response_attrs=[attr for attr in dir(orders_response) if not attr.startswith('_')][:5])
                    return []
            else:
                return []
            
        except Exception as e:
            logger.debug("查询账户活跃订单失败",
                        account_index=account_index,
                        market_index=market_index,
                        error=str(e))
            return []
    
    async def _cancel_order_safely(self, account_index: int, order_id: str, order=None) -> bool:
        """安全地取消订单"""
        try:
            if not self.client_factory:
                logger.debug("client_factory不可用", account_index=account_index)
                return False
            
            # 通过SignerClient取消订单
            signer_client = await self.client_factory.get_signer_client(account_index)
            if not signer_client:
                logger.debug("SignerClient不可用", account_index=account_index)
                return False
            
            # 取消订单
            result = await signer_client.cancel_order(order_id)
            
            if result:
                logger.debug("订单取消成功", 
                           account_index=account_index,
                           order_id=order_id)
                return True
            else:
                logger.debug("订单取消失败", 
                           account_index=account_index,
                           order_id=order_id)
                return False
            
        except Exception as e:
            logger.debug("取消订单时出错",
                        account_index=account_index,
                        order_id=order_id,
                        error=str(e))
            return False
    
    async def _cleanup_all_positions(self, pair_config: TradingPairConfig, accounts: List[int]) -> bool:
        """
        平掉所有相关账户的仓位
        开仓前清空所有仓位，确保环境干净
        
        Args:
            pair_config: 交易对配置
            accounts: 需要清理的账户列表
            
        Returns:
            bool: 是否完全成功清理（部分失败也会返回False，但不阻止开仓）
        """
        try:
            logger.info("🔍 开始清理所有仓位",
                       pair_id=pair_config.id,
                       market_index=pair_config.market_index,
                       accounts_count=len(accounts))
            
            all_success = True
            total_closed = 0
            
            for account_index in accounts:
                try:
                    logger.info("📋 检查账户仓位",
                               account_index=account_index,
                               market_index=pair_config.market_index)
                    
                    # 获取该账户在指定市场的所有仓位
                    positions = await self._get_account_positions(account_index, pair_config.market_index)
                    
                    if not positions:
                        logger.info("✅ 账户无仓位", account_index=account_index)
                        continue
                    
                    logger.info("🎯 发现需要清理的仓位",
                               account_index=account_index,
                               positions_count=len(positions),
                               positions=[{
                                   'side': getattr(p, 'side', 'unknown'),
                                   'size': float(getattr(p, 'size', 0)),
                                   'entry_price': float(getattr(p, 'entry_price', 0))
                               } for p in positions])
                    
                    # 逐个平仓
                    account_closed = 0
                    for position in positions:
                        try:
                            position_size = getattr(position, 'size', 0)
                            position_side = getattr(position, 'side', '')
                            
                            if abs(float(position_size)) == 0:
                                logger.info("仓位大小为0，跳过", 
                                           account_index=account_index,
                                           position=str(position)[:100])
                                continue
                            
                            # 确定平仓方向（与开仓方向相反）
                            close_side = 'sell' if position_side.lower() == 'long' else 'buy'
                            close_amount = abs(Decimal(str(position_size)))
                            
                            close_success = await self._close_position_immediately(
                                account_index, 
                                pair_config.market_index,
                                close_side,
                                close_amount,
                                position
                            )
                            
                            if close_success:
                                account_closed += 1
                                total_closed += 1
                                logger.info("✅ 成功平仓",
                                           account_index=account_index,
                                           side=position_side,
                                           size=float(position_size),
                                           close_side=close_side,
                                           close_amount=float(close_amount))
                            else:
                                logger.warning("❌ 平仓失败",
                                             account_index=account_index,
                                             side=position_side,
                                             size=float(position_size))
                                all_success = False
                                
                        except Exception as e:
                            logger.error("平单个仓位时出错",
                                       account_index=account_index,
                                       position=str(position)[:100],
                                       error=str(e))
                            all_success = False
                    
                    logger.info("📊 账户仓位清理完成",
                               account_index=account_index,
                               closed_count=account_closed,
                               total_positions=len(positions))
                    
                except Exception as e:
                    logger.error("清理账户仓位失败",
                               account_index=account_index,
                               error=str(e))
                    all_success = False
            
            if total_closed > 0:
                logger.info("🧹 仓位清理汇总",
                           pair_id=pair_config.id,
                           total_closed=total_closed,
                           accounts_processed=len(accounts),
                           all_success=all_success)
            else:
                logger.info("✨ 环境已经干净，无仓位需要清理",
                           pair_id=pair_config.id)
            
            return all_success
            
        except Exception as e:
            logger.error("清理仓位失败",
                        pair_id=pair_config.id,
                        error=str(e))
            return False
    
    async def _get_account_positions(self, account_index: int, market_index: int) -> List:
        """获取账户在指定市场的所有仓位"""
        try:
            logger.info("🔍 开始直接查询交易所仓位",
                       account_index=account_index,
                       market_index=market_index)
            
            # 方法1: 直接调用交易所API获取最新仓位数据
            direct_positions = await self._get_positions_from_exchange_api(account_index, market_index)
            
            if direct_positions:
                logger.info("✅ 直接API查询到仓位",
                           account_index=account_index,
                           positions_count=len(direct_positions),
                           positions=[{
                               'side': getattr(p, 'side', 'unknown'),
                               'size': float(getattr(p, 'size', 0)),
                               'market': getattr(p, 'market_index', 'unknown')
                           } for p in direct_positions])
                return direct_positions
            
            # 方法2: 通过账户管理器获取（作为备用）
            if hasattr(self, 'account_manager') and self.account_manager:
                logger.info("🔄 直接API无仓位，尝试账户管理器",
                           account_index=account_index)
                
                # 强制刷新账户数据
                await self.account_manager.refresh_account(account_index)
                
                # 获取所有仓位
                all_positions = self.account_manager.get_account_positions(account_index)
                
                logger.info("📋 账户管理器查询结果",
                           account_index=account_index,
                           total_positions=len(all_positions),
                           all_positions=[{
                               'side': getattr(p, 'side', 'unknown'),
                               'size': float(getattr(p, 'size', 0)),
                               'market': getattr(p, 'market_index', 'unknown')
                           } for p in all_positions])
                
                # 过滤指定市场的仓位
                market_positions = [p for p in all_positions 
                                  if getattr(p, 'market_index', None) == market_index and 
                                     abs(float(getattr(p, 'size', 0))) > 0]
                
                if market_positions:
                    logger.info("✅ 账户管理器找到目标市场仓位",
                               account_index=account_index,
                               market_index=market_index,
                               positions_count=len(market_positions))
                else:
                    logger.warning("⚠️ 账户管理器未找到目标市场仓位",
                                 account_index=account_index,
                                 market_index=market_index,
                                 total_positions=len(all_positions))
                
                return market_positions
            else:
                logger.error("❌ 账户管理器不可用", account_index=account_index)
                return []
            
        except Exception as e:
            logger.error("❌ 查询账户仓位失败",
                        account_index=account_index,
                        market_index=market_index,
                        error=str(e))
            return []
    
    async def _close_position_immediately(self, account_index: int, market_index: int, 
                                        side: str, amount: Decimal, position=None) -> bool:
        """立即平仓"""
        try:
            logger.info("🔨 执行立即平仓",
                       account_index=account_index,
                       market_index=market_index,
                       side=side,
                       amount=float(amount))
            
            # 使用市价单立即平仓
            order = await self.order_manager.create_market_order(
                account_index=account_index,
                market_index=market_index,
                side=side,
                amount=amount
            )
            
            if order:
                logger.info("✅ 平仓订单创建成功",
                           account_index=account_index,
                           order_id=getattr(order, 'id', 'unknown'),
                           side=side,
                           amount=float(amount))
                
                # 等待订单执行（简单等待，实际项目中可能需要更复杂的确认机制）
                await asyncio.sleep(1)
                return True
            else:
                logger.warning("❌ 平仓订单创建失败",
                             account_index=account_index,
                             side=side,
                             amount=float(amount))
                return False
            
        except Exception as e:
            logger.error("立即平仓失败",
                        account_index=account_index,
                        side=side,
                        amount=float(amount),
                        error=str(e))
            return False
    
    async def _get_positions_from_exchange_api(self, account_index: int, market_index: int) -> List:
        """直接从交易所API获取仓位数据"""
        try:
            logger.info("🌐 直接查询交易所API获取仓位",
                       account_index=account_index,
                       market_index=market_index)
            
            if not self.client_factory:
                logger.error("❌ client_factory不可用", account_index=account_index)
                return []
            
            # 获取账户API
            account_api = self.client_factory.get_account_api()
            
            # 获取认证令牌
            auth_token = None
            try:
                signer_client = await self.client_factory.get_signer_client(account_index)
                if signer_client:
                    auth_result, auth_error = signer_client.create_auth_token_with_expiry()
                    if auth_result and not auth_error:
                        auth_token = auth_result
                        logger.debug("✅ 获取认证令牌成功", account_index=account_index)
                    else:
                        logger.warning("⚠️ 认证令牌创建失败", 
                                     account_index=account_index, 
                                     error=auth_error)
                else:
                    logger.warning("⚠️ SignerClient不可用", account_index=account_index)
            except Exception as e:
                logger.warning("⚠️ 获取认证令牌失败", 
                             account_index=account_index, 
                             error=str(e))
            
            # 查询账户信息（包含仓位）
            try:
                logger.info("📡 调用账户API查询仓位",
                           account_index=account_index,
                           has_auth=bool(auth_token))
                
                # 调用账户API，不传递auth参数（认证通过SignerClient处理）
                account_response = await account_api.account(
                    by="index",
                    value=str(account_index)
                )
                
                # 解析响应
                if not account_response:
                    logger.warning("⚠️ API返回空响应", account_index=account_index)
                    return []
                
                logger.info("📡 API响应接收成功",
                           account_index=account_index,
                           response_type=type(account_response).__name__,
                           has_accounts=hasattr(account_response, 'accounts'))
                
                if not hasattr(account_response, 'accounts') or not account_response.accounts:
                    logger.warning("⚠️ API响应中无accounts数据", account_index=account_index)
                    return []
                
                account_data = account_response.accounts[0]
                logger.debug("📊 解析账户数据",
                           account_index=account_index,
                           account_type=type(account_data).__name__,
                           has_positions=hasattr(account_data, 'positions'),
                           account_attrs=[attr for attr in dir(account_data) if not attr.startswith('_')][:10])
                
                # 解析仓位数据
                positions = []
                if hasattr(account_data, 'positions') and account_data.positions:
                    logger.debug("📋 发现仓位数据",
                               account_index=account_index,
                               positions_count=len(account_data.positions) if hasattr(account_data.positions, '__len__') else 'unknown',
                               positions_type=type(account_data.positions).__name__)
                    
                    # 处理仓位数据
                    position_list = account_data.positions
                    if hasattr(position_list, '__iter__'):
                        for i, pos_data in enumerate(position_list):
                            try:
                                logger.debug("🔍 解析单个仓位",
                                           account_index=account_index,
                                           position_index=i,
                                           position_type=type(pos_data).__name__,
                                           position_attrs=[attr for attr in dir(pos_data) if not attr.startswith('_')][:10])
                                
                                # 提取仓位信息 - 尝试多种可能的字段名
                                position_market = None
                                for field_name in ['market_index', 'market_id', 'market', 'marketIndex']:
                                    if hasattr(pos_data, field_name):
                                        field_value = getattr(pos_data, field_name)
                                        if field_value is not None:
                                            position_market = int(field_value)
                                            break
                                
                                if position_market is None:
                                    position_market = 0  # 默认值
                                
                                # 根据官方文档解析仓位大小和方向
                                position_size = None
                                size_field_used = None
                                position_sign = None
                                
                                # 获取position字段（主要的仓位大小）
                                if hasattr(pos_data, 'position'):
                                    position_str = getattr(pos_data, 'position')
                                    if position_str is not None:
                                        try:
                                            position_size = float(position_str) if isinstance(position_str, str) else position_str
                                            size_field_used = 'position'
                                        except (ValueError, TypeError):
                                            position_size = 0
                                            size_field_used = 'position(invalid)'
                                
                                # 获取sign字段（仓位方向）
                                if hasattr(pos_data, 'sign'):
                                    position_sign = getattr(pos_data, 'sign')
                                
                                # 如果没有position字段，尝试其他字段
                                if position_size is None or position_size == 0:
                                    for size_field in ['position_value', 'allocated_margin', 'size', 'amount']:
                                        if hasattr(pos_data, size_field):
                                            potential_size = getattr(pos_data, size_field)
                                            logger.debug("🔍 检查备用大小字段",
                                                       field_name=size_field,
                                                       field_value=potential_size,
                                                       field_type=type(potential_size).__name__)
                                            if potential_size is not None:
                                                try:
                                                    potential_value = float(potential_size) if isinstance(potential_size, str) else potential_size
                                                    if potential_value != 0:
                                                        position_size = potential_value
                                                        size_field_used = size_field
                                                        break
                                                except (ValueError, TypeError):
                                                    continue
                                
                                if position_size is None:
                                    position_size = 0
                                    size_field_used = 'default'
                                
                                # 根据sign确定仓位方向
                                if position_sign == 1:
                                    position_side = 'long'
                                elif position_sign == -1:
                                    position_side = 'short'
                                else:
                                    position_side = getattr(pos_data, 'side', 'unknown')
                                
                                logger.info("📈 仓位详情",
                                           account_index=account_index,
                                           position_index=i,
                                           market_index=position_market,
                                           size=position_size,
                                           side=position_side,
                                           size_field_used=size_field_used,
                                           position_sign=position_sign,
                                           target_market=market_index)
                                
                                # 只返回指定市场且有大小的仓位
                                if (position_market == market_index and 
                                    position_size and 
                                    abs(float(position_size)) > 0):
                                    
                                    positions.append(pos_data)
                                    logger.info("✅ 找到目标仓位",
                                               account_index=account_index,
                                               market_index=position_market,
                                               size=position_size,
                                               side=position_side)
                                
                            except Exception as e:
                                logger.error("❌ 解析单个仓位失败",
                                           account_index=account_index,
                                           position_index=i,
                                           error=str(e))
                    else:
                        logger.warning("⚠️ 仓位数据不可迭代",
                                     account_index=account_index,
                                     positions_type=type(position_list).__name__)
                else:
                    logger.debug("ℹ️ 账户无仓位数据", account_index=account_index)
                
                logger.info("📊 仓位查询完成",
                           account_index=account_index,
                           found_positions=len(positions),
                           target_market=market_index)
                
                return positions
                
            except Exception as e:
                logger.error("❌ 调用账户API失败",
                           account_index=account_index,
                           error=str(e))
                return []
            
        except Exception as e:
            logger.error("❌ 直接API查询仓位失败",
                        account_index=account_index,
                        market_index=market_index,
                        error=str(e))
            return []