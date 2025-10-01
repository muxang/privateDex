"""
Hedge Trading Engine for Lighter Protocol
"""

import asyncio
from typing import Dict, List, Optional, Set
from datetime import datetime, timedelta
from decimal import Decimal
import structlog

from src.models import (
    HedgePosition, PositionStatus, TradingPairConfig, 
    TradingOpportunity, MarketData, OrderBook, SystemStatus, HedgeStrategy
)
from src.config.config_manager import ConfigManager
from src.services.account_manager import AccountManager
from src.services.order_manager import OrderManager
from src.services.risk_manager import RiskManager, RiskCheckResult
from src.services.websocket_manager import WebSocketManager
from src.strategies.balanced_hedge_strategy import BalancedHedgeStrategy
from src.utils.logger import trade_logger

logger = structlog.get_logger()


class HedgeTradingEngine:
    """Main hedge trading engine"""
    
    def __init__(self, config_manager: ConfigManager):
        self.config_manager = config_manager
        self.account_manager = AccountManager(config_manager)
        self.websocket_manager = WebSocketManager(config_manager)
        # 传入WebSocket管理器给OrderManager，实现数据共享
        self.order_manager = OrderManager(config_manager, self.account_manager, self.websocket_manager)
        self.risk_manager = RiskManager(config_manager, self.account_manager)
        
        # Initialize hedge strategies
        self.balanced_hedge_strategy = BalancedHedgeStrategy(
            self.order_manager, 
            self.account_manager, 
            client_factory=self.account_manager.client_factory
        )
        
        self.is_running = False
        self.active_positions: Dict[str, HedgePosition] = {}
        self.trading_pairs: Dict[str, TradingPairConfig] = {}
        self.last_trade_times: Dict[str, datetime] = {}
        
        # 统一仓位问题检测跟踪器（包括不一致和数量不平衡）
        self.position_issues_tracker: Dict[str, Dict] = {}
        
        # 止损止盈订单丢失跟踪器
        self.sl_tp_missing_tracker: Dict[str, Dict] = {}
        
        
        self._monitoring_task: Optional[asyncio.Task] = None
        self._position_management_task: Optional[asyncio.Task] = None
        
    async def initialize(self) -> None:
        """Initialize the trading engine"""
        try:
            logger.info("🚀 初始化对冲交易引擎...")
            
            # Initialize all managers
            await self.account_manager.initialize()
            await self.order_manager.initialize()
            await self.risk_manager.initialize()
            await self.websocket_manager.initialize()
            
            # Setup WebSocket callbacks for real-time updates
            self.websocket_manager.add_market_data_callback(self._on_market_data_update)
            self.websocket_manager.add_orderbook_callback(self._on_orderbook_update)
            self.websocket_manager.add_account_callback(self._on_account_update)
            
            # Load trading pairs
            self.load_trading_pairs()
            
            # Subscribe to market data for all active pairs
            await self._subscribe_to_market_data()
            
            # Setup risk event handling
            self.risk_manager.add_risk_event_handler(self._handle_risk_event)
            
            logger.info("✅ 对冲交易引擎初始化完成")
            
        except Exception as e:
            logger.error("❌ 对冲交易引擎初始化失败", error=str(e))
            raise
    
    def load_trading_pairs(self) -> None:
        """Load trading pairs configuration"""
        pairs = self.config_manager.get_active_pairs()
        for pair in pairs:
            self.trading_pairs[pair.id] = pair
            logger.info("交易对已加载", 
                       pair_id=pair.id,
                       name=pair.name,
                       market_index=pair.market_index)
    
    async def start(self) -> None:
        """Start the trading engine"""
        if self.is_running:
            logger.warning("交易引擎已在运行")
            return
        
        try:
            self.is_running = True
            
            # Start monitoring tasks
            await self._start_monitoring()
            await self._start_position_management()
            
            logger.info("🎯 对冲交易引擎已启动")
            
        except Exception as e:
            logger.error("启动交易引擎失败", error=str(e))
            self.is_running = False
            raise
    
    async def stop(self) -> None:
        """Stop the trading engine"""
        if not self.is_running:
            return
        
        try:
            self.is_running = False
            
            # Stop monitoring tasks
            await self._stop_monitoring()
            await self._stop_position_management()
            
            logger.info("🛑 对冲交易引擎已停止")
            
        except Exception as e:
            logger.error("停止交易引擎失败", error=str(e))
    
    async def _start_monitoring(self) -> None:
        """Start market monitoring"""
        if self._monitoring_task:
            self._monitoring_task.cancel()
        
        interval = self.config_manager.get_monitoring_interval()
        self._monitoring_task = asyncio.create_task(
            self._monitoring_loop(interval)
        )
        logger.info("市场监控已启动", interval_seconds=interval)
    
    async def _stop_monitoring(self) -> None:
        """Stop market monitoring"""
        if self._monitoring_task:
            self._monitoring_task.cancel()
            self._monitoring_task = None
    
    async def _start_position_management(self) -> None:
        """Start position management"""
        if self._position_management_task:
            self._position_management_task.cancel()
        
        self._position_management_task = asyncio.create_task(
            self._position_management_loop()
        )
        logger.info("仓位管理已启动")
    
    async def _stop_position_management(self) -> None:
        """Stop position management"""
        if self._position_management_task:
            self._position_management_task.cancel()
            self._position_management_task = None
    
    async def _monitoring_loop(self, interval: int) -> None:
        """Main monitoring loop"""
        while self.is_running:
            try:
                await self._check_trading_opportunities()
                await asyncio.sleep(interval)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("监控循环出错", error=str(e))
                await asyncio.sleep(interval)
    
    async def _position_management_loop(self) -> None:
        """Position management loop"""
        while self.is_running:
            try:
                await self._manage_active_positions()
                await asyncio.sleep(5)  # Check positions every 5 seconds
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("仓位管理循环出错", error=str(e))
                await asyncio.sleep(5)
    
    async def _check_trading_opportunities(self) -> None:
        """Check for trading opportunities"""
        logger.debug("🔍 开始检查交易机会",
                   trading_pairs_count=len(self.trading_pairs),
                   is_engine_running=self.is_running)
        
        for pair_id, pair_config in self.trading_pairs.items():
            try:
                if not pair_config.is_enabled:
                    continue
                
                logger.info("📊 检查交易对",
                           pair_id=pair_id,
                           is_enabled=pair_config.is_enabled,
                           market_index=pair_config.market_index,
                           leverage=pair_config.leverage,
                           max_positions=pair_config.max_positions)
                
                # Check if we can open a new position
                can_open = await self._can_open_new_position(pair_config)
                
                logger.debug("🎯 开仓条件检查结果",
                           pair_id=pair_id,
                           can_trade=can_open,
                           result="✅ 可以开仓" if can_open else "❌ 不能开仓")
                
                if can_open:
                    # Check market conditions
                    opportunity = await self._evaluate_market_conditions(pair_config)
                    if opportunity and opportunity.is_valid:
                        logger.info("🚀 发现交易机会，准备开仓",
                                   pair_id=pair_id,
                                   trigger_price=float(opportunity.trigger_price),
                                   confidence=float(opportunity.confidence))
                        
                        await self._execute_hedge_position(pair_config, opportunity)
                        
            except Exception as e:
                logger.error("检查交易机会失败",
                           pair_id=pair_id,
                           error=str(e))
        
        logger.debug("🔍 交易机会检查完成")
    
    async def _can_open_new_position(self, pair_config: TradingPairConfig) -> bool:
        """Check if we can open a new position for the pair"""
        try:
            logger.debug("🔍 开始详细条件检查", pair_id=pair_config.id)
            
            # 1. Check if there are any opening positions
            opening_positions = [p for p in self.active_positions.values()
                               if p.pair_id == pair_config.id and p.status == PositionStatus.OPENING]
            
            logger.debug("📋 条件1 - 检查正在开仓的仓位",
                       pair_id=pair_config.id,
                       opening_positions_count=len(opening_positions),
                       active_positions_total=len(self.active_positions),
                       result="✅ 通过" if len(opening_positions) == 0 else "❌ 失败")
            
            if opening_positions:
                logger.info("❌ 条件1失败: 有正在开仓的仓位，跳过新开仓",
                           pair_id=pair_config.id,
                           opening_positions_count=len(opening_positions))
                return False
            
            # 2. Check pending orders
            has_pending = await self._check_pending_orders(pair_config)
            
            logger.debug("📋 条件2 - 检查待处理订单",
                       pair_id=pair_config.id,
                       has_pending=has_pending,
                       result="✅ 通过" if not has_pending else "❌ 失败")
            
            if has_pending:
                logger.info("❌ 条件2失败: 账户有待处理订单，跳过新开仓",
                           pair_id=pair_config.id)
                return False
            
            # 3. Check account availability
            available_accounts = await self._check_account_availability(pair_config)
            
            logger.debug("📋 条件3 - 检查账户可用性",
                       pair_id=pair_config.id,
                       available_accounts_count=len(available_accounts),
                       required_accounts=2,
                       result="✅ 通过" if len(available_accounts) >= 2 else "❌ 失败")
            
            if len(available_accounts) < 2:
                logger.info("❌ 条件3失败: 可用账户不足",
                           pair_id=pair_config.id,
                           available_accounts_count=len(available_accounts))
                return False
            
            # 4. Check position overlap prevention
            has_active_positions = await self._check_position_overlap(pair_config)
            
            logger.debug("📋 条件4 - 检查仓位重叠防护",
                       pair_id=pair_config.id,
                       has_active_positions=has_active_positions,
                       result="✅ 通过" if not has_active_positions else "❌ 失败")
            
            if has_active_positions:
                logger.info("❌ 条件4失败: 存在活跃仓位，等待平仓后再开仓",
                           pair_id=pair_config.id)
                return False
            
            # 5. Check risk limits (使用预估的保证金金额)
            # 由于现在使用动态保证金计算，这里传递一个预估值用于风险检查
            estimated_margin = Decimal('1000')  # 预估1000美元作为风险检查基准
            risk_check = await self.risk_manager.check_open_position_risk(
                pair_config, estimated_margin
            )
            
            logger.debug("📋 条件5 - 检查风控限制",
                       pair_id=pair_config.id,
                       risk_allowed=risk_check.allowed,
                       risk_reason=risk_check.reason,
                       result="✅ 通过" if risk_check.allowed else "❌ 失败")
            
            if not risk_check.allowed:
                logger.info("❌ 条件5失败: 风控检查不通过",
                           pair_id=pair_config.id,
                           reason=risk_check.reason)
                return False
            
            # 6. Check cooldown period
            is_in_cooldown = self._is_in_cooldown(pair_config.id, pair_config.cooldown_minutes)
            
            logger.debug("📋 条件6 - 检查冷却时间",
                       pair_id=pair_config.id,
                       in_cooldown=is_in_cooldown,
                       cooldown_minutes=pair_config.cooldown_minutes,
                       result="✅ 通过" if not is_in_cooldown else "❌ 失败")
            
            if is_in_cooldown:
                logger.info("❌ 条件6失败: 仍在冷却期内",
                           pair_id=pair_config.id,
                           cooldown_minutes=pair_config.cooldown_minutes)
                return False
            
            logger.debug("🎉 所有开仓条件检查通过！", pair_id=pair_config.id)
            return True
            
        except Exception as e:
            logger.error("开仓条件检查失败", pair_id=pair_config.id, error=str(e))
            return False
    
    async def _check_pending_orders(self, pair_config: TradingPairConfig) -> bool:
        """
        Check if there are pending OPENING orders for the pair accounts (exclude stop-loss/take-profit)
        
        新增功能：
        - 自动清理超时订单，防止永久卡住
        - 使用配置的order_timeout设置
        """
        try:
            if not self.order_manager.has_signer_clients():
                logger.debug("没有SignerClients，跳过待处理订单检查",
                           pair_id=pair_config.id)
                return False
            
            # 获取配置的订单超时时间
            try:
                trading_config = self.config_manager.get_trading_engine_config()
                order_timeout = trading_config.get('order_timeout', 30)  # 默认30秒
                logger.info("获取订单超时配置", 
                           order_timeout=order_timeout,
                           trading_config=trading_config)
            except Exception as e:
                logger.warning("获取trading_engine配置失败，使用默认值", error=str(e))
                order_timeout = 30
            
            pair_accounts = self.config_manager.get_pair_accounts(pair_config)
            blocking_orders_count = 0
            total_pending_count = 0
            expired_orders_count = 0
            
            logger.debug("开始检查账户待处理订单",
                       pair_id=pair_config.id,
                       accounts_count=len(pair_accounts),
                       order_timeout=order_timeout)
            
            for account in pair_accounts:
                logger.info("检查账户待处理订单",
                           account_index=account.index,
                           order_timeout=order_timeout)
                
                # 使用带超时检查的get_pending_orders方法
                pending_orders = await self.order_manager.get_pending_orders(
                    account.index, 
                    timeout_seconds=order_timeout
                )
                
                logger.info("账户待处理订单查询结果",
                           account_index=account.index,
                           pending_count=len(pending_orders),
                           order_ids=[getattr(o, 'id', 'unknown') for o in pending_orders])
                
                if pending_orders:
                    total_pending_count += len(pending_orders)
                    
                    # 只检查开仓类订单（市价单、限价单），忽略止损止盈订单
                    for order in pending_orders:
                        order_id = getattr(order, 'id', '')
                        order_age = (datetime.now() - order.created_at).total_seconds()
                        
                        # 排除止损止盈订单（订单ID包含sl_或tp_前缀）
                        if not (order_id.startswith('sl_') or order_id.startswith('tp_')):
                            blocking_orders_count += 1
                            logger.info("发现阻塞性待处理订单",
                                       account_index=account.index,
                                       order_id=order_id,
                                       order_type=getattr(order, 'order_type', 'unknown'),
                                       order_age_seconds=int(order_age),
                                       timeout_seconds=order_timeout)
            
            # 记录统计信息
            if total_pending_count > 0 or expired_orders_count > 0:
                logger.debug("待处理订单检查统计",
                           pair_id=pair_config.id,
                           total_pending=total_pending_count,
                           blocking_orders=blocking_orders_count,
                           sl_tp_orders=total_pending_count - blocking_orders_count,
                           order_timeout_seconds=order_timeout,
                           check_result="阻塞" if blocking_orders_count > 0 else "通过")
            
            return blocking_orders_count > 0
            
        except Exception as e:
            logger.error("检查待处理订单失败", pair_id=pair_config.id, error=str(e))
            return True  # Conservative: assume there are pending orders
    
    async def _check_account_availability(self, pair_config: TradingPairConfig) -> List[int]:
        """Check which accounts are available for trading"""
        available_accounts = []
        pair_accounts = self.config_manager.get_pair_accounts(pair_config)
        
        for account_config in pair_accounts:
            if not account_config.is_active:
                logger.debug("账户未激活，跳过",
                           account_index=account_config.index)
                continue
                
            account = self.account_manager.get_account(account_config.index)
            if not account:
                logger.debug("未找到账户数据，跳过",
                           account_index=account_config.index)
                continue
            
            # Check balance - 使用交易对的最小余额要求，而不是账户配置的
            required_min_balance = pair_config.risk_limits.min_balance
            if account.available_balance < required_min_balance:
                logger.info("账户余额不足，跳过",
                           account_index=account_config.index,
                           available_balance=float(account.available_balance),
                           required_min_balance=float(required_min_balance),
                           account_config_min_balance=float(account_config.risk_limits.min_balance))
                continue
            else:
                logger.debug("账户余额检查通过",
                           account_index=account_config.index,
                           available_balance=float(account.available_balance),
                           required_min_balance=float(required_min_balance))
            
            # Check if account is in cooldown
            if self.account_manager.is_account_in_cooldown(account_config.index):
                continue
            
            # Check daily trade limits
            daily_count = self.account_manager.get_daily_trade_count(account_config.index)
            if daily_count >= account_config.max_daily_trades:
                continue
            
            available_accounts.append(account_config.index)
        
        return available_accounts
    
    def _is_in_cooldown(self, pair_id: str, cooldown_minutes: int) -> bool:
        """Check if pair is in cooldown period"""
        if pair_id not in self.last_trade_times:
            return False
        
        last_trade = self.last_trade_times[pair_id]
        cooldown_until = last_trade + timedelta(minutes=cooldown_minutes)
        return datetime.now() < cooldown_until
    
    async def _evaluate_market_conditions(self, pair_config: TradingPairConfig) -> Optional[TradingOpportunity]:
        """Evaluate market conditions for trading opportunity"""
        try:
            logger.debug("📊 开始检查市场条件", pair_id=pair_config.id)
            
            # Get market data
            market_data = await self.order_manager.get_market_data(pair_config.market_index)
            if not market_data:
                logger.info("❌ 市场条件检查失败: 无法获取价格数据",
                           pair_id=pair_config.id,
                           market_index=pair_config.market_index)
                return None
            
            logger.info("📊 价格数据检查",
                       pair_id=pair_config.id,
                       current_price=float(market_data.price),
                       result="✅ 价格数据有效")
            
            # Get orderbook
            orderbook = await self.order_manager.get_orderbook(pair_config.market_index)
            if not orderbook or not orderbook.bids or not orderbook.asks:
                logger.info("❌ 市场条件检查失败: 订单簿数据不足",
                           pair_id=pair_config.id)
                return None
            
            # Check spread
            best_bid = orderbook.bids[0]["price"]
            best_ask = orderbook.asks[0]["price"]
            spread = best_ask - best_bid
            spread_percent = spread / market_data.price
            
            max_spread = pair_config.price_conditions.max_spread_percent
            
            logger.info("📊 价差检查",
                       pair_id=pair_config.id,
                       best_bid=float(best_bid),
                       best_ask=float(best_ask),
                       spread_percent=float(spread_percent * 100),
                       max_spread_percent=float(max_spread * 100),
                       result="✅ 价差合理" if spread_percent <= max_spread else "❌ 价差过大")
            
            if spread_percent > max_spread:
                logger.info("❌ 市场条件检查失败: 价差过大",
                           pair_id=pair_config.id,
                           spread_percent=float(spread_percent * 100),
                           max_spread_percent=float(max_spread * 100))
                return None
            
            # Check liquidity
            bid_depth = sum(bid["amount"] for bid in orderbook.bids[:5])  # Top 5 levels
            ask_depth = sum(ask["amount"] for ask in orderbook.asks[:5])
            min_depth = pair_config.price_conditions.min_liquidity_depth
            
            logger.info("📊 流动性检查",
                       pair_id=pair_config.id,
                       bid_depth=float(bid_depth),
                       ask_depth=float(ask_depth),
                       min_depth=float(min_depth),
                       result="✅ 流动性充足" if min(bid_depth, ask_depth) >= min_depth else "❌ 流动性不足")
            
            if min(bid_depth, ask_depth) < min_depth:
                logger.info("❌ 市场条件检查失败: 流动性不足",
                           pair_id=pair_config.id,
                           min_depth_available=float(min(bid_depth, ask_depth)),
                           required_depth=float(min_depth))
                return None
            
            logger.info("🎉 市场条件检查全部通过！", pair_id=pair_config.id)
            
            # Create trading opportunity
            opportunity = TradingOpportunity(
                pair_id=pair_config.id,
                is_valid=True,
                trigger_price=market_data.price,
                conditions_met=[
                    "价格数据有效",
                    "价差合理",
                    "流动性充足"
                ],
                confidence=Decimal("0.8"),  # High confidence when all checks pass
                timestamp=datetime.now(),
                metadata={
                    "market_price": float(market_data.price),
                    "spread_percent": float(spread_percent),
                    "bid_depth": float(bid_depth),
                    "ask_depth": float(ask_depth)
                }
            )
            
            return opportunity
            
        except Exception as e:
            logger.error("市场条件评估失败", pair_id=pair_config.id, error=str(e))
            return None
    
    async def _execute_hedge_position(
        self, 
        pair_config: TradingPairConfig, 
        opportunity: TradingOpportunity
    ) -> None:
        """Execute hedge position opening using strategy"""
        try:
            logger.info("🚀 开始执行对冲开仓",
                       pair_id=pair_config.id,
                       strategy=pair_config.hedge_strategy.value,
                       trigger_price=float(opportunity.trigger_price))
            
            # Get available accounts
            available_accounts = await self._check_account_availability(pair_config)
            
            if len(available_accounts) < 2:
                logger.error("可用账户不足，无法执行对冲开仓",
                           pair_id=pair_config.id,
                           available_accounts=len(available_accounts))
                return
            
            # Execute hedge strategy based on configuration
            if pair_config.hedge_strategy == HedgeStrategy.BALANCED:
                hedge_position = await self.balanced_hedge_strategy.execute_hedge_open(
                    pair_config=pair_config,
                    accounts=available_accounts,
                    trigger_price=opportunity.trigger_price
                )
            else:
                logger.error("不支持的对冲策略",
                           pair_id=pair_config.id,
                           strategy=pair_config.hedge_strategy.value)
                return
            
            if hedge_position:
                # Store the hedge position
                self.active_positions[hedge_position.id] = hedge_position
                
                # Update last trade time
                self.last_trade_times[pair_config.id] = datetime.now()
                
                logger.info("✅ 对冲仓位创建成功",
                           position_id=hedge_position.id,
                           pair_id=pair_config.id,
                           strategy=hedge_position.strategy.value,
                           positions_count=len(hedge_position.positions))
                
                # 阶段1：开仓后立即同步订单状态
                await self._sync_position_orders_immediately(hedge_position, "开仓完成")
            else:
                logger.error("❌ 对冲仓位创建失败",
                           pair_id=pair_config.id)
                
        except Exception as e:
            logger.error("执行对冲开仓失败", pair_id=pair_config.id, error=str(e))
    
    async def _manage_active_positions(self) -> None:
        """Manage active positions"""
        for position_id, position in list(self.active_positions.items()):
            try:
                if position.status == PositionStatus.ACTIVE:
                    await self._monitor_position(position)
                elif position.status in [PositionStatus.CLOSED, PositionStatus.FAILED]:
                    # Remove completed positions
                    del self.active_positions[position_id]
                    
            except Exception as e:
                logger.error("管理仓位失败", position_id=position_id, error=str(e))
    
    async def _monitor_position(self, position: HedgePosition) -> None:
        """Monitor and manage a single position using strategy"""
        try:
            # 智能化监控：仅在必要时同步订单状态
            should_sync = self._should_sync_orders_now(position)
            if should_sync:
                logger.info("🔍 触发智能订单同步", 
                           position_id=position.id,
                           reason=should_sync)
                await self._sync_position_orders_immediately(position, f"智能监控: {should_sync}")
            
            # Update position PnL and market data using strategy
            if position.strategy == HedgeStrategy.BALANCED:
                await self.balanced_hedge_strategy.monitor_hedge_position(position)
                
                # Check if position should be closed
                pair_config = self.trading_pairs.get(position.pair_id)
                if pair_config:
                    should_close = await self.balanced_hedge_strategy.should_close_position(position, pair_config)
                    
                    if should_close:
                        logger.info("🔄 触发仓位平仓条件",
                                   position_id=position.id,
                                   pair_id=position.pair_id,
                                   total_pnl=float(position.total_pnl or 0))
                        
                        # Execute position close
                        success = await self.balanced_hedge_strategy.execute_hedge_close(position)
                        
                        if success:
                            logger.info("✅ 仓位平仓成功",
                                       position_id=position.id,
                                       final_pnl=float(position.total_pnl or 0))
                            
                            # 阶段1：平仓后立即同步订单状态
                            await self._sync_position_orders_immediately(position, "平仓完成")
                        else:
                            logger.warning("⚠️ 仓位平仓失败",
                                          position_id=position.id)
            else:
                # Fallback for other strategies
                position.updated_at = datetime.now()
            
        except Exception as e:
            logger.error("监控仓位失败", position_id=position.id, error=str(e))
    
    async def _check_position_overlap(self, pair_config: TradingPairConfig) -> bool:
        """
        检查仓位重叠防护 - 确保不在已有仓位时开新仓
        优先以交易所实际状态为准，本地状态仅作参考
        新增：仓位不一致连续三次确认后自动解除阻塞机制
        """
        try:
            # 1. 首先检查交易所实际仓位和订单状态（权威数据源）
            has_exchange_blocking = False
            exchange_positions_detail = []
            exchange_orders_detail = []
            account_position_status = {}  # 记录每个账户的仓位状态
            
            pair_accounts = self.config_manager.get_pair_accounts(pair_config)
            
            for account in pair_accounts:
                try:
                    # 1a. 检查交易所实际仓位
                    actual_positions = await self._get_exchange_positions(account.index, pair_config.market_index)
                    account_position_status[account.index] = {
                        'has_position': len(actual_positions) > 0,
                        'positions_count': len(actual_positions),
                        'positions': actual_positions
                    }
                    
                    if actual_positions:
                        logger.info("🔍 检测到交易所实际仓位",
                                   pair_id=pair_config.id,
                                   account_index=account.index,
                                   positions_count=len(actual_positions),
                                   positions=[{
                                       'side': getattr(p, 'side', 'unknown'),
                                       'size': float(getattr(p, 'size', 0)),
                                       'entry_price': float(getattr(p, 'entry_price', 0))
                                   } for p in actual_positions])
                        has_exchange_blocking = True
                        exchange_positions_detail.extend(actual_positions)
                    
                    # 1b. 检查交易所未完成订单（排除止损止盈订单）
                    actual_orders = await self._get_exchange_pending_orders(account.index, pair_config.market_index)
                    # 过滤掉止损止盈订单，只关心主交易订单
                    main_orders = [order for order in actual_orders 
                                 if not self._is_stop_loss_take_profit_order(order)]
                    
                    if main_orders:
                        logger.info("🔍 检测到交易所未完成主订单",
                                   pair_id=pair_config.id,
                                   account_index=account.index,
                                   main_orders_count=len(main_orders),
                                   total_orders_count=len(actual_orders))
                        has_exchange_blocking = True
                        exchange_orders_detail.extend(main_orders)
                        
                except Exception as e:
                    logger.warning("⚠️ 查询交易所状态失败，采用保守策略",
                                 pair_id=pair_config.id,
                                 account_index=account.index,
                                 error=str(e))
                    has_exchange_blocking = True
                    break
            
            # 2. 统一检查仓位问题（包括不一致和数量不平衡）并实施三次确认机制
            position_issues_detected = await self._check_position_issues(
                pair_config, account_position_status, exchange_positions_detail
            )
            
            # 如果检测到仓位问题，检查是否已经连续三次确认
            if position_issues_detected:
                should_fix = await self._handle_position_issues(pair_config, account_position_status, exchange_positions_detail)
                if should_fix:
                    logger.warning("🔓 仓位问题已连续三次确认，强制重新开仓",
                                 pair_id=pair_config.id)
                    # 强制清理所有问题仓位和订单，然后允许重新开仓
                    await self._force_cleanup_problematic_positions(pair_config, account_position_status, exchange_positions_detail)
                    return False  # 允许重新开仓
            
            # 3. 检查止损止盈订单丢失情况（针对有仓位但缺乏保护的情况）
            sl_tp_missing_detected = await self._check_sl_tp_orders_missing(
                pair_config, account_position_status
            )
            
            # 如果检测到止损止盈订单丢失，检查是否需要重新开仓
            if sl_tp_missing_detected:
                should_reopen = await self._handle_sl_tp_missing(pair_config, account_position_status)
                if should_reopen:
                    logger.info("🔄 止损止盈订单连续丢失，将强制重新开仓",
                               pair_id=pair_config.id)
                    # 强制清理现有仓位，然后允许重新开仓
                    await self._force_cleanup_unprotected_positions(pair_config, account_position_status)
                    return False  # 允许重新开仓
            
            # 4. 检查本地缓存的活跃仓位（仅作参考和同步检查）
            pair_positions = [p for p in self.active_positions.values() 
                            if p.pair_id == pair_config.id]
            
            # 统计不同状态的仓位
            active_positions = [p for p in pair_positions if p.status == PositionStatus.ACTIVE]
            opening_positions = [p for p in pair_positions if p.status == PositionStatus.OPENING]
            closing_positions = [p for p in pair_positions if p.status == PositionStatus.CLOSING]
            
            logger.info("📋 本地仓位状态检查",
                        pair_id=pair_config.id,
                        active_count=len(active_positions),
                        opening_count=len(opening_positions),
                        closing_count=len(closing_positions),
                        total_positions=len(pair_positions))
            
            # 如果本地显示有仓位但交易所没有，说明本地数据过时，需要清理
            if pair_positions and not has_exchange_blocking:
                logger.warning("🔄 发现本地仓位与交易所不同步，本地显示有仓位但交易所无仓位", 
                             pair_id=pair_config.id,
                             local_positions_count=len(pair_positions),
                             exchange_has_positions=bool(exchange_positions_detail),
                             exchange_has_orders=bool(exchange_orders_detail))
                
                # 自动清理过时的本地仓位数据
                await self._cleanup_stale_local_positions(pair_config.id, pair_positions)
            
            # 4. 最终判断：以交易所实际状态为准
            has_blocking_positions = has_exchange_blocking
            
            if has_blocking_positions:
                blocking_details = []
                if exchange_positions_detail:
                    blocking_details.append(f"交易所仓位 {len(exchange_positions_detail)} 个")
                if exchange_orders_detail:
                    blocking_details.append(f"未完成主订单 {len(exchange_orders_detail)} 个")
                
                logger.info("🚫 仓位重叠检测: 发现阻塞条件",
                           pair_id=pair_config.id,
                           blocking_reason=" + ".join(blocking_details))
            else:
                logger.info("✅ 仓位重叠检测: 无阻塞，可以开仓",
                           pair_id=pair_config.id,
                           basis="以交易所实际状态为准")
                
                # 清除该交易对的不一致跟踪记录（因为现在状态正常）
                if pair_config.id in self.position_inconsistency_tracker:
                    del self.position_inconsistency_tracker[pair_config.id]
                    logger.info("🧹 清除仓位不一致跟踪记录", pair_id=pair_config.id)
            
            return has_blocking_positions
            
        except Exception as e:
            logger.error("仓位重叠检查失败", pair_id=pair_config.id, error=str(e))
            return True  # 保守策略：出错时假设有重叠
    
    async def _get_exchange_positions(self, account_index: int, market_index: int) -> List:
        """查询交易所实际仓位"""
        try:
            # 刷新账户信息以获取最新状态
            await self.account_manager.refresh_account(account_index)
            
            # 通过账户管理器获取实际仓位
            positions = self.account_manager.get_account_positions(account_index)
            
            # 过滤指定市场的有效仓位（仓位大小不为0）
            market_positions = [p for p in positions 
                              if p.market_index == market_index and 
                                 abs(float(p.size)) > 0]
            
            return market_positions
            
        except Exception as e:
            logger.debug("查询交易所仓位失败",
                        account_index=account_index,
                        market_index=market_index,
                        error=str(e))
            return []
    
    async def _get_exchange_pending_orders(self, account_index: int, market_index: int) -> List:
        """查询交易所未完成订单"""
        try:
            # 通过API直接查询交易所的实际订单状态
            if not self.order_manager.order_api:
                logger.debug("订单API不可用，跳过交易所订单查询",
                           account_index=account_index)
                return []
            
            # 使用OrderApi查询账户的未完成订单
            # 根据官方SDK文档，使用account_inactive_orders方法
            orders_response = await self.order_manager.order_api.account_inactive_orders(
                str(account_index), limit=1000  # Use positional argument and add required limit
            )
            
            if not orders_response or not hasattr(orders_response, 'orders'):
                return []
            
            # 过滤指定市场的未完成订单
            market_orders = []
            for order in orders_response.orders:
                # 检查是否为指定市场的订单
                if (hasattr(order, 'market_index') and 
                    getattr(order, 'market_index') == market_index):
                    
                    # 检查订单状态是否为未完成
                    order_status = getattr(order, 'status', '').lower()
                    if order_status in ['pending', 'partially_filled', 'open', 'active']:
                        market_orders.append(order)
            
            return market_orders
            
        except Exception as e:
            logger.debug("查询交易所订单失败",
                        account_index=account_index,
                        market_index=market_index,
                        error=str(e))
            return []
    
    def _is_stop_loss_take_profit_order(self, order) -> bool:
        """
        判断订单是否为止损止盈订单
        止损止盈订单不应该阻止新仓位开启
        """
        try:
            # 方法1：检查订单类型
            order_type = getattr(order, 'order_type', '').lower()
            if order_type in ['stop_loss', 'take_profit', 'stop', 'limit_stop', 'market_stop']:
                return True
            
            # 方法2：检查订单ID中是否包含止损止盈标识
            order_id = str(getattr(order, 'id', ''))
            if any(keyword in order_id.lower() for keyword in ['stop', 'profit', 'sl', 'tp']):
                return True
            
            # 方法3：检查订单的client_order_id
            client_order_id = str(getattr(order, 'client_order_id', ''))
            if any(keyword in client_order_id.lower() for keyword in ['stop', 'profit', 'sl', 'tp']):
                return True
            
            # 方法4：检查是否为条件订单 (conditional order)
            is_conditional = getattr(order, 'is_conditional', False)
            if is_conditional:
                return True
                
            return False
            
        except Exception as e:
            logger.debug("判断止损止盈订单时出错", 
                        order_id=getattr(order, 'id', 'unknown'),
                        error=str(e))
            return False  # 出错时保守处理，不认为是止损止盈订单
    
    async def _check_position_issues(
        self, 
        pair_config: TradingPairConfig, 
        account_position_status: Dict,
        exchange_positions: List
    ) -> bool:
        """统一检查仓位问题（包括不一致和数量不平衡）"""
        try:
            issues_detected = []
            
            # 1. 检查仓位不一致（原有逻辑）
            inconsistency_detected = await self._check_position_inconsistency(
                pair_config, account_position_status
            )
            if inconsistency_detected:
                issues_detected.append("position_inconsistency")
            
            # 2. 检查仓位数量不平衡（新增逻辑）
            if exchange_positions and len(exchange_positions) >= 2:
                size_imbalance_detected = self._check_hedge_position_size_balance(
                    pair_config, exchange_positions
                )
                if size_imbalance_detected:
                    issues_detected.append("size_imbalance")
            
            if issues_detected:
                logger.info("📊 检测到仓位问题",
                           pair_id=pair_config.id,
                           issues=issues_detected,
                           total_issues=len(issues_detected))
                return True
            
            return False
            
        except Exception as e:
            logger.error("统一仓位问题检查失败",
                        pair_id=pair_config.id,
                        error=str(e))
            return False

    def _check_hedge_position_size_balance(
        self, 
        pair_config: TradingPairConfig, 
        exchange_positions: List
    ) -> bool:
        """检查对冲仓位数量平衡性"""
        try:
            if len(exchange_positions) < 2:
                return False  # 少于2个仓位无法进行对冲平衡检查
            
            # 按账户分组仓位
            positions_by_account = {}
            for position in exchange_positions:
                account_index = getattr(position, 'account_index', None)
                if not account_index:
                    continue
                    
                if account_index not in positions_by_account:
                    positions_by_account[account_index] = []
                positions_by_account[account_index].append(position)
            
            if len(positions_by_account) < 2:
                return False  # 至少需要2个账户的仓位进行平衡检查
            
            # 计算每个账户的总仓位数量（绝对值）
            account_sizes = {}
            account_details = {}
            
            for account_index, positions in positions_by_account.items():
                total_size = 0
                position_details = []
                
                for pos in positions:
                    size = abs(float(getattr(pos, 'size', 0)))
                    side = getattr(pos, 'side', 'unknown')
                    entry_price = float(getattr(pos, 'entry_price', 0))
                    
                    total_size += size
                    position_details.append({
                        'side': side,
                        'size': size,
                        'entry_price': entry_price
                    })
                
                account_sizes[account_index] = total_size
                account_details[account_index] = position_details
            
            # 检查数量平衡性
            sizes = list(account_sizes.values())
            max_size = max(sizes)
            min_size = min(sizes)
            
            # 允许的最大差异比例（5%）
            max_allowed_diff_percent = 5.0
            
            if max_size > 0:
                diff_percent = ((max_size - min_size) / max_size) * 100
                
                logger.info("🔍 对冲仓位数量平衡检查",
                           pair_id=pair_config.id,
                           account_sizes=account_sizes,
                           max_size=max_size,
                           min_size=min_size,
                           diff_percent=round(diff_percent, 2),
                           max_allowed_percent=max_allowed_diff_percent,
                           is_balanced=diff_percent <= max_allowed_diff_percent)
                
                if diff_percent > max_allowed_diff_percent:
                    logger.warning("⚠️ 检测到对冲仓位数量不平衡",
                                 pair_id=pair_config.id,
                                 account_details=account_details,
                                 size_difference_percent=round(diff_percent, 2),
                                 threshold_percent=max_allowed_diff_percent)
                    return True
            
            return False
            
        except Exception as e:
            logger.error("检查对冲仓位数量平衡失败", 
                        pair_id=pair_config.id,
                        error=str(e))
            return False

    async def _handle_position_issues(
        self, 
        pair_config: TradingPairConfig, 
        account_position_status: Dict,
        exchange_positions: List
    ) -> bool:
        """处理仓位问题的三次检测机制"""
        try:
            pair_id = pair_config.id
            current_time = datetime.now()
            
            # 获取或创建跟踪记录
            if pair_id not in self.position_issues_tracker:
                self.position_issues_tracker[pair_id] = {
                    'count': 0,
                    'first_detected': current_time,
                    'last_detected': current_time,
                    'issues_history': [],
                    'positions_snapshot': []
                }
            
            tracker = self.position_issues_tracker[pair_id]
            
            # 更新检测次数和时间
            tracker['count'] += 1
            tracker['last_detected'] = current_time
            
            # 记录当前问题和仓位快照
            current_issues = []
            if await self._check_position_inconsistency(pair_config, account_position_status):
                current_issues.append("position_inconsistency")
            
            if exchange_positions and self._check_hedge_position_size_balance(pair_config, exchange_positions):
                current_issues.append("size_imbalance")
            
            tracker['issues_history'].append({
                'time': current_time,
                'issues': current_issues
            })
            
            # 记录仓位快照
            positions_snapshot = []
            for pos in exchange_positions:
                positions_snapshot.append({
                    'account_index': getattr(pos, 'account_index', None),
                    'side': getattr(pos, 'side', 'unknown'),
                    'size': float(getattr(pos, 'size', 0)),
                    'entry_price': float(getattr(pos, 'entry_price', 0))
                })
            
            tracker['positions_snapshot'] = positions_snapshot
            
            # 检测间隔不能太短（至少30秒）
            time_since_first = (current_time - tracker['first_detected']).total_seconds()
            min_detection_interval = 30  # 30秒
            
            logger.info("📊 仓位问题检测记录",
                       pair_id=pair_id,
                       detection_count=tracker['count'],
                       time_since_first_seconds=int(time_since_first),
                       min_interval_seconds=min_detection_interval,
                       current_issues=current_issues,
                       positions_snapshot=positions_snapshot)
            
            # 需要连续3次检测，且时间间隔合理
            if tracker['count'] >= 3 and time_since_first >= min_detection_interval:
                logger.warning("🚨 仓位问题已连续检测到3次，满足强制修复条件",
                             pair_id=pair_id,
                             total_detections=tracker['count'],
                             time_span_seconds=int(time_since_first),
                             issues_history=tracker['issues_history'][-3:],  # 显示最近3次检测
                             final_positions=positions_snapshot)
                
                # 重置跟踪器
                del self.position_issues_tracker[pair_id]
                return True
            
            return False
            
        except Exception as e:
            logger.error("处理仓位问题检测失败", 
                        pair_id=pair_config.id,
                        error=str(e))
            return False

    async def _force_cleanup_problematic_positions(
        self, 
        pair_config: TradingPairConfig, 
        account_position_status: Dict,
        exchange_positions: List
    ) -> None:
        """强制清理有问题的仓位"""
        try:
            logger.info("🧹 开始强制清理问题仓位",
                       pair_id=pair_config.id,
                       positions_to_cleanup=len(exchange_positions))
            
            # 获取涉及的账户
            affected_accounts = set()
            for pos in exchange_positions:
                account_index = getattr(pos, 'account_index', None)
                if account_index:
                    affected_accounts.add(account_index)
            
            # 同时也从account_position_status中获取账户
            for account_index in account_position_status.keys():
                affected_accounts.add(account_index)
            
            logger.info("🎯 将清理以下账户的仓位",
                       pair_id=pair_config.id,
                       affected_accounts=list(affected_accounts))
            
            # 使用现有的平衡对冲平仓方法
            from ..strategies.balanced_hedge_strategy import BalancedHedgeStrategy
            strategy = BalancedHedgeStrategy(
                config_manager=self.config_manager,
                order_manager=self.order_manager,
                account_manager=self.account_manager,
                risk_manager=self.risk_manager
            )
            
            # 对每个账户执行平仓
            for account_index in affected_accounts:
                try:
                    # 获取该账户在该市场的仓位
                    account_positions = await strategy._get_account_positions(
                        account_index, pair_config.market_index
                    )
                    
                    if account_positions:
                        logger.info("🔄 执行账户仓位清理",
                                   account_index=account_index,
                                   positions_count=len(account_positions))
                        
                        # 执行平仓
                        await strategy._close_all_positions_for_account(
                            account_index, pair_config.market_index
                        )
                        
                except Exception as e:
                    logger.error("清理账户仓位失败",
                               account_index=account_index,
                               error=str(e))
            
            logger.info("✅ 问题仓位清理完成",
                       pair_id=pair_config.id,
                       cleaned_accounts=list(affected_accounts))
            
        except Exception as e:
            logger.error("强制清理问题仓位失败",
                        pair_id=pair_config.id,
                        error=str(e))
    
    async def _check_position_inconsistency(
        self, 
        pair_config: TradingPairConfig, 
        account_position_status: Dict
    ) -> bool:
        """
        检查账户间仓位不一致情况
        返回True表示检测到不一致，False表示一致或无仓位
        """
        try:
            if not account_position_status:
                return False
            
            # 获取所有账户的仓位状态
            has_position_accounts = []
            no_position_accounts = []
            
            for account_index, status in account_position_status.items():
                if status['has_position']:
                    has_position_accounts.append(account_index)
                else:
                    no_position_accounts.append(account_index)
            
            # 如果所有账户都有仓位或都没有仓位，则认为一致
            total_accounts = len(account_position_status)
            if len(has_position_accounts) == total_accounts or len(no_position_accounts) == total_accounts:
                return False
            
            # 检测到不一致：部分账户有仓位，部分没有
            logger.warning("⚠️ 检测到账户间仓位不一致",
                          pair_id=pair_config.id,
                          total_accounts=total_accounts,
                          has_position_accounts=has_position_accounts,
                          no_position_accounts=no_position_accounts,
                          has_position_count=len(has_position_accounts),
                          no_position_count=len(no_position_accounts))
            
            return True
            
        except Exception as e:
            logger.error("检查仓位不一致失败", pair_id=pair_config.id, error=str(e))
            return False
    
    async def _handle_position_inconsistency(
        self, 
        pair_config: TradingPairConfig, 
        account_position_status: Dict
    ) -> bool:
        """
        处理仓位不一致情况，实施三次确认机制
        返回True表示应该解除阻塞，False表示继续阻塞
        """
        try:
            pair_id = pair_config.id
            current_time = datetime.now()
            
            # 创建当前状态的指纹用于比较
            status_fingerprint = self._create_position_status_fingerprint(account_position_status)
            
            # 初始化或获取跟踪记录
            if pair_id not in self.position_inconsistency_tracker:
                self.position_inconsistency_tracker[pair_id] = {
                    'first_detected': current_time,
                    'last_checked': current_time,
                    'consecutive_count': 1,
                    'last_fingerprint': status_fingerprint,
                    'status_history': [status_fingerprint]
                }
                
                logger.info("🔍 首次检测到仓位不一致，开始跟踪",
                           pair_id=pair_id,
                           status_fingerprint=status_fingerprint)
                return False
            
            tracker = self.position_inconsistency_tracker[pair_id]
            
            # 检查状态是否与上次相同
            if tracker['last_fingerprint'] == status_fingerprint:
                # 状态相同，增加连续计数
                tracker['consecutive_count'] += 1
                tracker['last_checked'] = current_time
                
                logger.info("📊 仓位不一致状态持续",
                           pair_id=pair_id,
                           consecutive_count=tracker['consecutive_count'],
                           first_detected=tracker['first_detected'].isoformat(),
                           duration_minutes=int((current_time - tracker['first_detected']).total_seconds() / 60))
                
                # 检查是否达到三次确认
                if tracker['consecutive_count'] >= 3:
                    logger.warning("🚨 仓位不一致已连续确认3次，将自动解除阻塞",
                                 pair_id=pair_id,
                                 consecutive_count=tracker['consecutive_count'],
                                 total_duration_minutes=int((current_time - tracker['first_detected']).total_seconds() / 60))
                    return True
                else:
                    remaining = 3 - tracker['consecutive_count']
                    logger.info(f"⏳ 仓位不一致跟踪中，还需要{remaining}次确认",
                               pair_id=pair_id,
                               current_count=tracker['consecutive_count'],
                               required_count=3)
                    return False
                    
            else:
                # 状态发生变化，重置计数
                logger.info("🔄 仓位不一致状态发生变化，重置跟踪计数",
                           pair_id=pair_id,
                           old_fingerprint=tracker['last_fingerprint'],
                           new_fingerprint=status_fingerprint,
                           previous_count=tracker['consecutive_count'])
                
                tracker['first_detected'] = current_time
                tracker['last_checked'] = current_time
                tracker['consecutive_count'] = 1
                tracker['last_fingerprint'] = status_fingerprint
                tracker['status_history'].append(status_fingerprint)
                
                # 限制历史记录长度
                if len(tracker['status_history']) > 10:
                    tracker['status_history'] = tracker['status_history'][-10:]
                
                return False
                
        except Exception as e:
            logger.error("处理仓位不一致失败", pair_id=pair_config.id, error=str(e))
            return False
    
    def _create_position_status_fingerprint(self, account_position_status: Dict) -> str:
        """
        为账户仓位状态创建指纹，用于检测状态变化
        """
        try:
            fingerprint_data = []
            for account_index in sorted(account_position_status.keys()):
                status = account_position_status[account_index]
                fingerprint_data.append(f"{account_index}:{status['has_position']}:{status['positions_count']}")
            
            return "|".join(fingerprint_data)
            
        except Exception as e:
            logger.error("创建仓位状态指纹失败", error=str(e))
            return f"error_{datetime.now().timestamp()}"
    
    async def _force_cleanup_inconsistent_positions(
        self, 
        pair_config: TradingPairConfig, 
        account_position_status: Dict
    ) -> None:
        """
        强制清理不一致的仓位和订单，为新开仓做准备
        """
        try:
            logger.info("🧹 开始强制清理不一致仓位",
                       pair_id=pair_config.id,
                       affected_accounts=list(account_position_status.keys()))
            
            cleanup_tasks = []
            
            for account_index, status in account_position_status.items():
                if status['has_position'] and status['positions']:
                    # 有仓位的账户：强制平仓
                    for position in status['positions']:
                        task = self._force_close_position(account_index, position, pair_config)
                        cleanup_tasks.append(task)
                
                # 所有账户：取消历史订单
                cancel_task = self.order_manager.cancel_all_inactive_orders(
                    account_index=account_index,
                    market_index=pair_config.market_index
                )
                cleanup_tasks.append(cancel_task)
            
            # 并行执行所有清理任务
            if cleanup_tasks:
                results = await asyncio.gather(*cleanup_tasks, return_exceptions=True)
                
                success_count = sum(1 for r in results if not isinstance(r, Exception))
                total_count = len(results)
                
                logger.info("🧹 强制清理完成",
                           pair_id=pair_config.id,
                           total_tasks=total_count,
                           success_count=success_count,
                           failed_count=total_count - success_count)
            
            # 清理本地仓位记录
            pair_positions = [p for p in self.active_positions.values() 
                            if p.pair_id == pair_config.id]
            for position in pair_positions:
                if position.id in self.active_positions:
                    del self.active_positions[position.id]
                    logger.info("🧹 清理本地仓位记录", 
                               pair_id=pair_config.id,
                               position_id=position.id)
            
            # 移除不一致跟踪记录
            if pair_config.id in self.position_inconsistency_tracker:
                del self.position_inconsistency_tracker[pair_config.id]
                logger.info("🧹 清除仓位不一致跟踪记录", pair_id=pair_config.id)
            
        except Exception as e:
            logger.error("强制清理不一致仓位失败", pair_id=pair_config.id, error=str(e))
    
    async def _force_close_position(self, account_index: int, position, pair_config: TradingPairConfig) -> bool:
        """
        强制平仓单个仓位
        """
        try:
            position_size = abs(float(getattr(position, 'size', 0)))
            if position_size <= 0:
                return True
            
            position_side = getattr(position, 'side', 'unknown')
            # 平仓方向与持仓方向相反
            close_side = "sell" if position_side.lower() == "long" else "buy"
            
            logger.info("🔨 强制平仓",
                       account_index=account_index,
                       market_index=pair_config.market_index,
                       position_side=position_side,
                       close_side=close_side,
                       size=position_size)
            
            # 使用市价单快速平仓
            order_info = await self.order_manager.create_market_order(
                account_index=account_index,
                market_index=pair_config.market_index,
                side=close_side,
                amount=Decimal(str(position_size))
            )
            
            if order_info:
                logger.info("✅ 强制平仓订单已提交",
                           account_index=account_index,
                           order_id=order_info.id)
                return True
            else:
                logger.error("❌ 强制平仓订单提交失败",
                           account_index=account_index)
                return False
                
        except Exception as e:
            logger.error("强制平仓失败",
                        account_index=account_index,
                        error=str(e))
            return False

    async def _cleanup_stale_local_positions(self, pair_id: str, stale_positions: List) -> None:
        """
        清理过时的本地仓位数据
        当交易所已无仓位但本地仍有记录时调用
        """
        try:
            logger.info("🧹 开始清理过时的本地仓位数据", 
                       pair_id=pair_id,
                       stale_count=len(stale_positions))
            
            cleaned_count = 0
            for position in stale_positions:
                try:
                    position_id = position.id
                    
                    # 将仓位状态标记为已关闭
                    position.status = PositionStatus.CLOSED
                    position.updated_at = datetime.now()
                    position.metadata = position.metadata or {}
                    position.metadata['auto_cleanup_reason'] = '交易所无对应仓位，自动清理'
                    position.metadata['cleanup_time'] = datetime.now().isoformat()
                    
                    # 从活跃仓位中移除
                    if position_id in self.active_positions:
                        del self.active_positions[position_id]
                        cleaned_count += 1
                        
                        logger.info("✅ 清理过时仓位", 
                                   pair_id=pair_id,
                                   position_id=position_id,
                                   original_status=getattr(position, '_original_status', 'unknown'))
                    
                except Exception as e:
                    logger.error("清理单个仓位失败",
                               pair_id=pair_id,
                               position_id=getattr(position, 'id', 'unknown'),
                               error=str(e))
            
            if cleaned_count > 0:
                logger.info("🎯 本地仓位清理完成",
                           pair_id=pair_id,
                           cleaned_count=cleaned_count,
                           total_stale=len(stale_positions))
            
        except Exception as e:
            logger.error("清理过时本地仓位失败",
                        pair_id=pair_id,
                        error=str(e))

    async def _check_sl_tp_orders_missing(
        self, 
        pair_config: TradingPairConfig, 
        account_position_status: Dict
    ) -> bool:
        """
        检查有仓位但缺乏止损止盈保护的情况
        返回True表示检测到止损止盈订单丢失
        """
        try:
            # 暂时禁用止损止盈保护检查
            logger.debug("⏸️ 止损止盈保护检查已暂时禁用",
                       pair_id=pair_config.id)
            return False
            
            # TODO: 重新启用时取消下面的注释
            # # 只检查有仓位的账户
            # accounts_with_positions = [
            #     account_index for account_index, status in account_position_status.items()
            #     if status['has_position'] and status['positions_count'] > 0
            # ]
            # 
            # if not accounts_with_positions:
            #     # 没有仓位，不需要检查止损止盈
            #     return False
            # 
            # logger.info("🔍 检查止损止盈订单完整性",
            #            pair_id=pair_config.id,
            #            accounts_with_positions=accounts_with_positions)
            # 
            # missing_protection_accounts = []
            # 
            # for account_index in accounts_with_positions:
            #     try:
            #         # 检查该账户的委托订单
            #         委托订单 = await self._get_exchange_sl_tp_orders(account_index, pair_config.market_index)
            #         
            #         position_count = account_position_status[account_index]['positions_count']
            #         
            #         logger.info("账户止损止盈检查",
            #                    account_index=account_index,
            #                    position_count=position_count,
            #                    委托订单数量=len(委托订单))
            #         
            #         # 简单判定：有仓位但委托订单少于2笔则认为缺乏保护
            #         if len(委托订单) < 2:
            #             missing_protection_accounts.append(account_index)
            #             logger.warning("⚠️ 账户仓位缺乏止损止盈保护",
            #                          account_index=account_index,
            #                          position_count=position_count,
            #                          委托订单数量=len(委托订单),
            #                          判定="委托订单不足2笔")
            #         else:
            #             logger.info("✅ 账户仓位止损止盈保护完整",
            #                        account_index=account_index,
            #                        position_count=position_count,
            #                        委托订单数量=len(委托订单),
            #                        判定="委托订单>=2笔")
            #     
            #     except Exception as e:
            #         logger.error("检查账户止损止盈失败",
            #                    account_index=account_index,
            #                    error=str(e))
            #         # 出错时保守处理，认为缺乏保护
            #         missing_protection_accounts.append(account_index)
            # 
            # if missing_protection_accounts:
            #     logger.warning("🚨 检测到止损止盈订单丢失",
            #                  pair_id=pair_config.id,
            #                  affected_accounts=missing_protection_accounts,
            #                  total_accounts_with_positions=len(accounts_with_positions))
            #     return True
            # else:
            #     logger.info("✅ 所有账户止损止盈保护完整",
            #                pair_id=pair_config.id)
            #     return False
                
        except Exception as e:
            logger.error("检查止损止盈订单失败", pair_id=pair_config.id, error=str(e))
            return False

    async def _get_exchange_sl_tp_orders(self, account_index: int, market_index: int) -> List:
        """从WebSocket获取委托订单，简单判定止损止盈保护"""
        try:
            logger.info("🔍 从WebSocket获取委托订单",
                       account_index=account_index,
                       market_index=market_index)
            
            # 首先确保WebSocket监听该账户
            if hasattr(self, 'websocket_manager') and self.websocket_manager:
                await self.websocket_manager.subscribe_account(account_index)
                
                # 等待短时间让WebSocket数据更新
                await asyncio.sleep(0.5)
                
                # 从WebSocket缓存中获取账户数据
                account_data = self.websocket_manager.latest_account_data.get(account_index)
                
                if account_data:
                    logger.debug("从WebSocket获取账户数据成功", 
                               account_index=account_index,
                               data_keys=list(account_data.keys()) if isinstance(account_data, dict) else "非字典类型",
                               sample_data=str(account_data)[:500] if account_data else "无数据")
                    
                    # 提取订单信息
                    market_orders = []
                    
                    # 检查不同可能的订单字段
                    possible_order_fields = ['orders', 'active_orders', 'pending_orders', 'open_orders']
                    orders_data = None
                    
                    for field in possible_order_fields:
                        if field in account_data:
                            orders_data = account_data[field]
                            logger.debug(f"找到订单字段: {field}", 
                                       account_index=account_index,
                                       orders_count=len(orders_data) if isinstance(orders_data, (list, dict)) else "非列表/字典")
                            break
                    
                    if orders_data:
                        # 处理订单数据
                        if isinstance(orders_data, list):
                            for order in orders_data:
                                try:
                                    if isinstance(order, dict):
                                        order_market = order.get('market_index') or order.get('market_id')
                                        if order_market == market_index:
                                            market_orders.append(order)
                                    else:
                                        # 如果是对象，尝试获取属性
                                        order_market = getattr(order, 'market_index', getattr(order, 'market_id', None))
                                        if order_market == market_index:
                                            market_orders.append(order)
                                except Exception as order_error:
                                    logger.debug("处理WebSocket订单时出错",
                                               account_index=account_index,
                                               error=str(order_error))
                                    continue
                        elif isinstance(orders_data, dict):
                            # 如果订单数据是字典格式，尝试提取
                            for order_id, order_info in orders_data.items():
                                try:
                                    if isinstance(order_info, dict):
                                        order_market = order_info.get('market_index') or order_info.get('market_id')
                                        if order_market == market_index:
                                            market_orders.append(order_info)
                                except Exception as order_error:
                                    logger.debug("处理WebSocket订单字典时出错",
                                               account_index=account_index,
                                               order_id=order_id,
                                               error=str(order_error))
                                    continue
                    
                    logger.info("WebSocket委托订单查询完成",
                               account_index=account_index,
                               market_index=market_index,
                               market_orders=len(market_orders),
                               使用数据源="WebSocket")
                    
                    # 简单判定逻辑：有2笔或以上委托订单就认为有止损止盈保护
                    if len(market_orders) >= 2:
                        logger.info("✅ 检测到足够的委托订单，认定有止损止盈保护",
                                   account_index=account_index,
                                   market_index=market_index,
                                   委托订单数量=len(market_orders))
                        return market_orders
                    else:
                        logger.info("⚠️ 委托订单数量不足，可能缺乏止损止盈保护",
                                   account_index=account_index,
                                   market_index=market_index,
                                   委托订单数量=len(market_orders))
                        return market_orders
                else:
                    logger.warning("WebSocket中未找到账户数据",
                                 account_index=account_index,
                                 available_accounts=list(self.websocket_manager.latest_account_data.keys()))
                    return []
            else:
                logger.warning("WebSocket管理器不可用", account_index=account_index)
                return []
            
        except Exception as e:
            logger.error("从WebSocket获取委托订单失败",
                        account_index=account_index,
                        market_index=market_index,
                        error=str(e))
            return []

    def _analyze_conditional_orders(self, orders: list) -> dict:
        """分析条件订单，尝试识别止损止盈订单对"""
        try:
            conditional_orders = []
            
            # 找出所有条件订单
            for order in orders:
                is_conditional = getattr(order, 'is_conditional', False)
                trigger_price = getattr(order, 'trigger_price', None)
                reduce_only = getattr(order, 'reduce_only', False)
                
                if is_conditional and trigger_price is not None:
                    conditional_orders.append({
                        'order': order,
                        'trigger_price': float(trigger_price),
                        'reduce_only': reduce_only,
                        'side': str(getattr(order, 'side', '')).lower(),
                        'order_type': str(getattr(order, 'order_type', '')).lower(),
                        'id': str(getattr(order, 'id', ''))
                    })
            
            if len(conditional_orders) >= 2:
                logger.info("发现条件订单对，尝试识别止损止盈",
                           conditional_orders_count=len(conditional_orders),
                           trigger_prices=[o['trigger_price'] for o in conditional_orders])
                
                # 按触发价格排序，通常低价为止损，高价为止盈（对于多头仓位）
                conditional_orders.sort(key=lambda x: x['trigger_price'])
                
                result = {
                    'stop_loss_orders': [],
                    'take_profit_orders': [],
                    'analysis': 'conditional_pairs'
                }
                
                # 简单启发式：reduce_only的订单更可能是止损止盈
                reduce_only_orders = [o for o in conditional_orders if o['reduce_only']]
                
                if len(reduce_only_orders) >= 2:
                    # 如果有多个reduce_only订单，按价格分类
                    reduce_only_orders.sort(key=lambda x: x['trigger_price'])
                    result['stop_loss_orders'] = [reduce_only_orders[0]['order']]  # 低价的作为止损
                    result['take_profit_orders'] = [reduce_only_orders[-1]['order']]  # 高价的作为止盈
                elif len(reduce_only_orders) == 1:
                    # 只有一个reduce_only订单，其他条件订单也考虑
                    result['stop_loss_orders'] = [reduce_only_orders[0]['order']]
                    # 找其他条件订单作为止盈
                    other_orders = [o for o in conditional_orders if not o['reduce_only']]
                    if other_orders:
                        result['take_profit_orders'] = [other_orders[0]['order']]
                else:
                    # 没有reduce_only标志，基于价格和类型推测
                    # 这种情况下较难准确判断，但可以尝试
                    if len(conditional_orders) >= 2:
                        result['stop_loss_orders'] = [conditional_orders[0]['order']]  # 低价
                        result['take_profit_orders'] = [conditional_orders[-1]['order']]  # 高价
                
                return result
            
            return {'stop_loss_orders': [], 'take_profit_orders': [], 'analysis': 'insufficient_orders'}
            
        except Exception as e:
            logger.debug("分析条件订单失败", error=str(e))
            return {'stop_loss_orders': [], 'take_profit_orders': [], 'analysis': 'error'}

    def _is_stop_loss_order(self, order) -> bool:
        """判断是否为止损订单（基于Lighter Protocol订单字段）"""
        try:
            # 获取订单基本信息
            order_type = str(getattr(order, 'order_type', '')).lower()
            order_subtype = str(getattr(order, 'order_subtype', '')).lower()
            is_conditional = getattr(order, 'is_conditional', False)
            trigger_price = getattr(order, 'trigger_price', None)
            order_id = str(getattr(order, 'id', ''))
            client_order_id = str(getattr(order, 'client_order_id', ''))
            side = str(getattr(order, 'side', '')).lower()
            
            # 方法1: 明确的止损订单类型标识
            stop_loss_indicators = ['stop_loss', 'stop-loss', 'stoploss', 'sl']
            if any(indicator in order_type for indicator in stop_loss_indicators):
                return True
            if any(indicator in order_subtype for indicator in stop_loss_indicators):
                return True
            
            # 方法2: 检查我们系统创建的止损订单ID格式
            sl_id_patterns = ['sl_', 'stop_loss_', 'stop-loss-']
            if (any(pattern in order_id.lower() for pattern in sl_id_patterns) or
                any(pattern in client_order_id.lower() for pattern in sl_id_patterns)):
                return True
            
            # 方法3: Lighter Protocol条件订单检查
            # 条件订单 + 有触发价格 + 特定的订单类型模式
            if is_conditional and trigger_price is not None:
                # 检查是否为条件限价单或条件市价单（常见的止损订单类型）
                conditional_types = ['conditional', 'stop', 'trigger']
                if any(ct in order_type for ct in conditional_types):
                    # 进一步验证：检查是否有reduce_only标志（减仓订单，常见于止损）
                    reduce_only = getattr(order, 'reduce_only', False)
                    if reduce_only:
                        return True
                    
                    # 或者检查订单属性中是否有其他止损标识
                    order_attrs = str(getattr(order, '__dict__', '')).lower()
                    if any(indicator in order_attrs for indicator in stop_loss_indicators):
                        return True
                        
            return False
                   
        except Exception as e:
            logger.debug("判断止损订单时出错",
                        order_id=getattr(order, 'id', 'unknown'),
                        error=str(e))
            return False

    def _is_take_profit_order(self, order) -> bool:
        """判断是否为止盈订单（基于Lighter Protocol订单字段）"""
        try:
            # 获取订单基本信息
            order_type = str(getattr(order, 'order_type', '')).lower()
            order_subtype = str(getattr(order, 'order_subtype', '')).lower()
            is_conditional = getattr(order, 'is_conditional', False)
            trigger_price = getattr(order, 'trigger_price', None)
            order_id = str(getattr(order, 'id', ''))
            client_order_id = str(getattr(order, 'client_order_id', ''))
            side = str(getattr(order, 'side', '')).lower()
            
            # 方法1: 明确的止盈订单类型标识
            take_profit_indicators = ['take_profit', 'take-profit', 'takeprofit', 'tp', 'profit']
            if any(indicator in order_type for indicator in take_profit_indicators):
                return True
            if any(indicator in order_subtype for indicator in take_profit_indicators):
                return True
            
            # 方法2: 检查我们系统创建的止盈订单ID格式
            tp_id_patterns = ['tp_', 'take_profit_', 'take-profit-', 'profit_']
            if (any(pattern in order_id.lower() for pattern in tp_id_patterns) or
                any(pattern in client_order_id.lower() for pattern in tp_id_patterns)):
                return True
            
            # 方法3: Lighter Protocol条件订单检查
            # 条件订单 + 有触发价格 + 特定的订单类型模式
            if is_conditional and trigger_price is not None:
                # 检查是否为条件限价单或条件市价单（常见的止盈订单类型）
                conditional_types = ['conditional', 'limit', 'trigger']
                if any(ct in order_type for ct in conditional_types):
                    # 进一步验证：检查是否有reduce_only标志（减仓订单，常见于止盈）
                    reduce_only = getattr(order, 'reduce_only', False)
                    if reduce_only:
                        return True
                    
                    # 或者检查订单属性中是否有其他止盈标识
                    order_attrs = str(getattr(order, '__dict__', '')).lower()
                    if any(indicator in order_attrs for indicator in take_profit_indicators):
                        return True
                        
            return False
                   
        except Exception as e:
            logger.debug("判断止盈订单时出错",
                        order_id=getattr(order, 'id', 'unknown'),
                        error=str(e))
            return False

    async def _handle_sl_tp_missing(
        self, 
        pair_config: TradingPairConfig, 
        account_position_status: Dict
    ) -> bool:
        """
        处理止损止盈订单丢失情况，实施三次确认机制
        返回True表示应该重新开仓，False表示继续等待
        """
        try:
            pair_id = pair_config.id
            current_time = datetime.now()
            
            # 创建当前状态的指纹
            status_fingerprint = self._create_sl_tp_missing_fingerprint(account_position_status)
            
            # 初始化或获取跟踪记录
            if pair_id not in self.sl_tp_missing_tracker:
                self.sl_tp_missing_tracker[pair_id] = {
                    'first_detected': current_time,
                    'last_checked': current_time,
                    'consecutive_count': 1,
                    'last_fingerprint': status_fingerprint,
                    'status_history': [status_fingerprint]
                }
                
                logger.info("🔍 首次检测到止损止盈订单丢失，开始跟踪",
                           pair_id=pair_id,
                           status_fingerprint=status_fingerprint)
                return False
            
            tracker = self.sl_tp_missing_tracker[pair_id]
            
            # 检查状态是否与上次相同
            if tracker['last_fingerprint'] == status_fingerprint:
                # 状态相同，增加连续计数
                tracker['consecutive_count'] += 1
                tracker['last_checked'] = current_time
                
                logger.warning("📊 止损止盈订单持续丢失",
                             pair_id=pair_id,
                             consecutive_count=tracker['consecutive_count'],
                             first_detected=tracker['first_detected'].isoformat(),
                             duration_minutes=int((current_time - tracker['first_detected']).total_seconds() / 60))
                
                # 检查是否达到三次确认
                if tracker['consecutive_count'] >= 3:
                    logger.error("🚨 止损止盈订单连续丢失3次，将强制重新开仓",
                               pair_id=pair_id,
                               consecutive_count=tracker['consecutive_count'],
                               total_duration_minutes=int((current_time - tracker['first_detected']).total_seconds() / 60))
                    return True
                else:
                    remaining = 3 - tracker['consecutive_count']
                    logger.warning(f"⏳ 止损止盈订单丢失跟踪中，还需要{remaining}次确认",
                                 pair_id=pair_id,
                                 current_count=tracker['consecutive_count'],
                                 required_count=3)
                    return False
                    
            else:
                # 状态发生变化，重置计数
                logger.info("🔄 止损止盈保护状态发生变化，重置跟踪计数",
                           pair_id=pair_id,
                           old_fingerprint=tracker['last_fingerprint'],
                           new_fingerprint=status_fingerprint,
                           previous_count=tracker['consecutive_count'])
                
                tracker['first_detected'] = current_time
                tracker['last_checked'] = current_time
                tracker['consecutive_count'] = 1
                tracker['last_fingerprint'] = status_fingerprint
                tracker['status_history'].append(status_fingerprint)
                
                # 限制历史记录长度
                if len(tracker['status_history']) > 10:
                    tracker['status_history'] = tracker['status_history'][-10:]
                
                return False
                
        except Exception as e:
            logger.error("处理止损止盈订单丢失失败", pair_id=pair_config.id, error=str(e))
            return False

    def _create_sl_tp_missing_fingerprint(self, account_position_status: Dict) -> str:
        """
        为止损止盈保护状态创建指纹
        """
        try:
            fingerprint_data = []
            for account_index in sorted(account_position_status.keys()):
                status = account_position_status[account_index]
                has_position = status['has_position']
                # 这里简化处理，实际应该包含止损止盈订单的详细信息
                fingerprint_data.append(f"{account_index}:pos={has_position}")
            
            return "|".join(fingerprint_data)
            
        except Exception as e:
            logger.error("创建止损止盈状态指纹失败", error=str(e))
            return f"error_{datetime.now().timestamp()}"

    async def _force_cleanup_unprotected_positions(
        self, 
        pair_config: TradingPairConfig, 
        account_position_status: Dict
    ) -> None:
        """
        强制清理没有止损止盈保护的仓位，为重新开仓做准备
        """
        try:
            logger.info("🧹 开始强制清理无保护仓位",
                       pair_id=pair_config.id,
                       affected_accounts=list(account_position_status.keys()))
            
            cleanup_tasks = []
            
            for account_index, status in account_position_status.items():
                if status['has_position'] and status['positions']:
                    # 有仓位的账户：强制平仓
                    for position in status['positions']:
                        task = self._force_close_position(account_index, position, pair_config)
                        cleanup_tasks.append(task)
                
                # 所有账户：取消所有订单（包括残留的止损止盈）
                cancel_task = self.order_manager.cancel_all_inactive_orders(
                    account_index=account_index,
                    market_index=pair_config.market_index
                )
                cleanup_tasks.append(cancel_task)
            
            # 并行执行所有清理任务
            if cleanup_tasks:
                results = await asyncio.gather(*cleanup_tasks, return_exceptions=True)
                
                success_count = sum(1 for r in results if not isinstance(r, Exception))
                total_count = len(results)
                
                logger.info("🧹 无保护仓位清理完成",
                           pair_id=pair_config.id,
                           total_tasks=total_count,
                           success_count=success_count,
                           failed_count=total_count - success_count)
            
            # 清理本地仓位记录
            pair_positions = [p for p in self.active_positions.values() 
                            if p.pair_id == pair_config.id]
            for position in pair_positions:
                if position.id in self.active_positions:
                    del self.active_positions[position.id]
                    logger.info("🧹 清理本地仓位记录", 
                               pair_id=pair_config.id,
                               position_id=position.id)
            
            # 移除止损止盈丢失跟踪记录
            if pair_config.id in self.sl_tp_missing_tracker:
                del self.sl_tp_missing_tracker[pair_config.id]
                logger.info("🧹 清除止损止盈丢失跟踪记录", pair_id=pair_config.id)
            
        except Exception as e:
            logger.error("强制清理无保护仓位失败", pair_id=pair_config.id, error=str(e))
    
    async def _subscribe_to_market_data(self) -> None:
        """Subscribe to market data for all active trading pairs and accounts"""
        try:
            # Subscribe to market data for active pairs
            for pair_config in self.trading_pairs.values():
                if pair_config.is_enabled:
                    await self.websocket_manager.subscribe_market(pair_config.market_index)
                    logger.info("订阅市场数据",
                               pair_id=pair_config.id,
                               market_index=pair_config.market_index)
            
            # Subscribe to account updates for all active accounts
            accounts = self.config_manager.get_active_accounts()
            for account in accounts:
                await self.websocket_manager.subscribe_account(account.index)
                logger.info("订阅账户数据",
                           account_index=account.index)
                           
        except Exception as e:
            logger.error("订阅数据失败", error=str(e))
    
    def _on_market_data_update(self, market_data: MarketData) -> None:
        """Handle real-time market data updates"""
        try:
            # Update strategies with latest market data
            if hasattr(self.balanced_hedge_strategy, 'update_market_data'):
                self.balanced_hedge_strategy.update_market_data(market_data)
            
            # Log price updates for active pairs
            active_pairs = [p for p in self.trading_pairs.values() 
                          if p.market_index == market_data.market_index]
            
            for pair in active_pairs:
                logger.debug("实时价格更新",
                           pair_id=pair.id,
                           market_index=market_data.market_index,
                           price=float(market_data.price),
                           bid=float(market_data.bid_price) if market_data.bid_price else None,
                           ask=float(market_data.ask_price) if market_data.ask_price else None)
                
                # Check if any active positions need attention
                pair_positions = [p for p in self.active_positions.values()
                                if p.pair_id == pair.id and p.status == PositionStatus.ACTIVE]
                
                if pair_positions:
                    # Update position prices with latest market data
                    for hedge_position in pair_positions:
                        for position in hedge_position.positions:
                            if position.market_index == market_data.market_index:
                                position.current_price = market_data.price
                                position.updated_at = datetime.now()
                
        except Exception as e:
            logger.error("处理市场数据更新失败",
                        market_index=market_data.market_index,
                        error=str(e))
    
    def _on_orderbook_update(self, orderbook: OrderBook) -> None:
        """Handle real-time orderbook updates"""
        try:
            # Update strategies with latest orderbook
            if hasattr(self.balanced_hedge_strategy, 'update_orderbook'):
                self.balanced_hedge_strategy.update_orderbook(orderbook)
            
            # Log orderbook updates for debugging
            best_bid = orderbook.bids[0]['price'] if orderbook.bids else None
            best_ask = orderbook.asks[0]['price'] if orderbook.asks else None
            
            logger.debug("订单簿更新",
                        market_index=orderbook.market_index,
                        best_bid=float(best_bid) if best_bid else None,
                        best_ask=float(best_ask) if best_ask else None,
                        spread=float(best_ask - best_bid) if best_bid and best_ask else None)
                
        except Exception as e:
            logger.error("处理订单簿更新失败",
                        market_index=orderbook.market_index,
                        error=str(e))
    
    def _on_account_update(self, account_data: dict) -> None:
        """Handle real-time account updates"""
        try:
            account_index = account_data.get('account_index')
            if account_index is None:
                return
            
            # Update account manager with latest data
            if hasattr(self.account_manager, 'update_account_data'):
                self.account_manager.update_account_data(account_index, account_data)
            
            # Log account updates
            balance = account_data.get('balance')
            available_balance = account_data.get('available_balance')
            
            logger.debug("账户实时更新",
                        account_index=account_index,
                        balance=float(balance) if balance else None,
                        available_balance=float(available_balance) if available_balance else None)
            
            # Check for margin calls or low balance warnings
            if balance and float(balance) < 100:  # Low balance threshold
                logger.warning("账户余额过低",
                             account_index=account_index,
                             balance=float(balance))
            
            # 阶段2：检测订单状态变化，触发自动同步
            # 检查是否有position_count变化（可能表明订单执行）
            position_count = account_data.get('position_count')
            order_count = account_data.get('order_count') 
            
            if position_count is not None or order_count is not None:
                # 异步触发订单同步检查（不阻塞WebSocket回调）
                asyncio.create_task(self._auto_sync_on_account_change(account_index, account_data))
                             
        except Exception as e:
            logger.error("处理账户更新失败",
                        account_data=account_data,
                        error=str(e))
    
    def _handle_risk_event(self, event) -> None:
        """Handle risk events"""
        logger.warning("处理风险事件",
                      event_type=event.type,
                      severity=event.severity,
                      action=event.action)
        
        if event.action == "emergency_stop":
            # Emergency stop the engine
            asyncio.create_task(self.stop())
    
    async def _auto_sync_on_account_change(self, account_index: int, account_data: dict) -> None:
        """
        阶段2：检测到账户变化时自动同步相关订单状态
        """
        try:
            logger.debug("🔄 检测账户变化，开始自动同步",
                        account_index=account_index,
                        position_count=account_data.get('position_count'),
                        order_count=account_data.get('order_count'))
            
            # 查找该账户相关的活跃仓位
            related_positions = []
            for position in self.active_positions.values():
                if hasattr(position, 'positions'):
                    for pos in position.positions:
                        if getattr(pos, 'account_index', None) == account_index:
                            related_positions.append(position)
                            break
            
            if not related_positions:
                logger.debug("未找到相关活跃仓位，跳过自动同步",
                           account_index=account_index)
                return
            
            # 为相关仓位触发同步
            sync_tasks = []
            for position in related_positions:
                task = self._sync_position_orders_immediately(position, "账户更新触发")
                sync_tasks.append(task)
            
            # 并行执行同步任务
            if sync_tasks:
                await asyncio.gather(*sync_tasks, return_exceptions=True)
                
                logger.info("✅ 基于账户更新的自动同步完成",
                           account_index=account_index,
                           synced_positions=len(sync_tasks))
                           
        except Exception as e:
            logger.error("账户变化自动同步失败",
                        account_index=account_index,
                        error=str(e))

    async def _sync_position_orders_immediately(self, position, operation: str) -> None:
        """
        阶段1：在关键操作后立即同步订单状态
        同步指定仓位的所有订单状态
        """
        try:
            logger.info("🔄 开始立即同步订单状态",
                       position_id=position.id,
                       operation=operation,
                       positions_count=len(getattr(position, 'positions', [])))
            
            sync_tasks = []
            
            # 收集该仓位所有订单信息
            if hasattr(position, 'positions'):
                for pos in position.positions:
                    account_index = getattr(pos, 'account_index', None)
                    if account_index is not None:
                        # 获取市场索引
                        market_index = getattr(pos, 'market_index', 1)  # 默认为1
                        
                        # 同步主订单
                        if hasattr(pos, 'order_id') and pos.order_id:
                            task = self.balanced_hedge_strategy._synchronize_order_status(
                                pos.order_id, account_index, market_index
                            )
                            sync_tasks.append(task)
                        
                        # 同步止损订单
                        if hasattr(pos, 'stop_loss_order_id') and pos.stop_loss_order_id:
                            task = self.balanced_hedge_strategy._synchronize_order_status(
                                pos.stop_loss_order_id, account_index, market_index
                            )
                            sync_tasks.append(task)
                        
                        # 同步止盈订单
                        if hasattr(pos, 'take_profit_order_id') and pos.take_profit_order_id:
                            task = self.balanced_hedge_strategy._synchronize_order_status(
                                pos.take_profit_order_id, account_index, market_index
                            )
                            sync_tasks.append(task)
            
            # 并行执行所有同步任务
            if sync_tasks:
                results = await asyncio.gather(*sync_tasks, return_exceptions=True)
                
                successful_syncs = sum(1 for r in results if not isinstance(r, Exception))
                failed_syncs = len(results) - successful_syncs
                
                # 检查并清理已完成的止损止盈订单
                await self._update_order_confirmation_status(position, results)
                await self._cleanup_completed_orders(position, results, sync_tasks)
                
                logger.info("✅ 订单状态同步完成",
                           position_id=position.id,
                           operation=operation,
                           total_orders=len(sync_tasks),
                           successful_syncs=successful_syncs,
                           failed_syncs=failed_syncs)
                
                if failed_syncs > 0:
                    logger.warning("⚠️ 部分订单同步失败",
                                  position_id=position.id,
                                  failed_syncs=failed_syncs,
                                  errors=[str(r) for r in results if isinstance(r, Exception)])
            else:
                logger.debug("没有需要同步的订单",
                           position_id=position.id,
                           operation=operation)
                
        except Exception as e:
            logger.error("立即订单同步失败",
                        position_id=getattr(position, 'id', 'unknown'),
                        operation=operation,
                        error=str(e))

    async def _cleanup_completed_orders(self, position, sync_results, sync_tasks) -> None:
        """
        检查并清理已完成的止损止盈订单
        当订单状态为filled/completed/executed时，从仓位记录中移除订单ID
        """
        try:
            if not hasattr(position, 'positions'):
                return
            
            cleaned_orders = []
            
            for pos in position.positions:
                account_index = getattr(pos, 'account_index', None)
                if account_index is None:
                    continue
                
                # 直接查询每个订单的状态
                order_checks = []
                if hasattr(pos, 'stop_loss_order_id') and pos.stop_loss_order_id:
                    order_checks.append(('stop_loss_order_id', pos.stop_loss_order_id))
                if hasattr(pos, 'take_profit_order_id') and pos.take_profit_order_id:
                    order_checks.append(('take_profit_order_id', pos.take_profit_order_id))
                
                # 检查每个订单的状态
                for order_attr, order_id in order_checks:
                    try:
                        # 获取市场索引
                        market_index = getattr(pos, 'market_index', 1)
                        
                        # 直接查询订单状态
                        order_status = await self.balanced_hedge_strategy._synchronize_order_status(
                            order_id, account_index, market_index
                        )
                        
                        # 如果订单已完成，清理订单ID并实现OCO机制
                        if order_status and order_status.lower() in ['filled', 'completed', 'executed']:
                            logger.info("检测到已完成的订单，开始清理和OCO处理",
                                       position_id=position.id,
                                       order_type=order_attr,
                                       order_id=order_id,
                                       status=order_status,
                                       account_index=account_index)
                            
                            # OCO机制：取消对应的另一个订单
                            await self._cancel_paired_order(pos, order_attr, account_index)
                            
                            # 清理订单ID
                            setattr(pos, order_attr, None)
                            cleaned_orders.append({
                                'type': order_attr,
                                'order_id': order_id,
                                'status': order_status,
                                'account_index': account_index
                            })
                        
                    except Exception as e:
                        logger.debug("查询订单状态失败",
                                   order_id=order_id,
                                   order_type=order_attr,
                                   error=str(e))
            
            if cleaned_orders:
                logger.info("✅ 已完成订单清理完成",
                           position_id=position.id,
                           cleaned_count=len(cleaned_orders),
                           cleaned_orders=cleaned_orders)
                
                # 更新仓位状态
                position.updated_at = datetime.now()
                
                # 检查是否所有止损止盈订单都已完成
                await self._check_position_completion_status(position)
            
        except Exception as e:
            logger.error("清理已完成订单失败",
                        position_id=getattr(position, 'id', 'unknown'),
                        error=str(e))

    async def _cancel_paired_order(self, position_detail, triggered_order_type: str, account_index: int) -> None:
        """
        OCO机制：当一个止损/止盈订单触发后，取消对应的另一个订单
        """
        try:
            paired_order_id = None
            paired_order_type = None
            
            # 确定需要取消的配对订单
            if triggered_order_type == 'stop_loss_order_id':
                # 止损触发了，取消止盈订单
                if hasattr(position_detail, 'take_profit_order_id') and position_detail.take_profit_order_id:
                    paired_order_id = position_detail.take_profit_order_id
                    paired_order_type = 'take_profit_order_id'
            elif triggered_order_type == 'take_profit_order_id':
                # 止盈触发了，取消止损订单
                if hasattr(position_detail, 'stop_loss_order_id') and position_detail.stop_loss_order_id:
                    paired_order_id = position_detail.stop_loss_order_id
                    paired_order_type = 'stop_loss_order_id'
            
            if paired_order_id:
                logger.info("🔄 OCO机制：开始取消配对订单",
                           triggered_order=triggered_order_type,
                           paired_order_id=paired_order_id,
                           paired_order_type=paired_order_type,
                           account_index=account_index)
                
                # 尝试取消配对订单
                try:
                    # 首先检查配对订单是否还在活跃状态
                    # 这里需要从position_detail获取市场索引
                    market_index = getattr(position_detail, 'market_index', 1)
                    paired_status = await self.balanced_hedge_strategy._synchronize_order_status(
                        paired_order_id, account_index, market_index
                    )
                    
                    if paired_status and paired_status.lower() in ['pending', 'open', 'active']:
                        # 订单还活跃，需要取消
                        cancel_success = await self.order_manager.cancel_order(paired_order_id)
                        
                        if cancel_success:
                            logger.info("✅ OCO成功：配对订单已取消",
                                       paired_order_id=paired_order_id,
                                       paired_order_type=paired_order_type)
                            
                            # 清理配对订单ID
                            setattr(position_detail, paired_order_type, None)
                        else:
                            logger.warning("⚠️ OCO失败：配对订单取消失败",
                                          paired_order_id=paired_order_id,
                                          paired_order_type=paired_order_type)
                    else:
                        logger.info("ℹ️ OCO跳过：配对订单已非活跃状态",
                                   paired_order_id=paired_order_id,
                                   paired_status=paired_status)
                        
                        # 如果订单已经非活跃，直接清理ID
                        setattr(position_detail, paired_order_type, None)
                        
                except Exception as cancel_error:
                    logger.error("OCO取消订单失败",
                                paired_order_id=paired_order_id,
                                error=str(cancel_error))
                    
        except Exception as e:
            logger.error("OCO机制处理失败",
                        triggered_order=triggered_order_type,
                        account_index=account_index,
                        error=str(e))

    async def _check_position_completion_status(self, position) -> None:
        """
        检查仓位完成状态
        如果所有止损止盈订单都已执行，考虑将仓位标记为完成
        """
        try:
            if not hasattr(position, 'positions'):
                return
            
            # 检查是否还有活跃的止损止盈订单
            has_active_orders = False
            
            for pos in position.positions:
                if ((hasattr(pos, 'stop_loss_order_id') and pos.stop_loss_order_id) or
                    (hasattr(pos, 'take_profit_order_id') and pos.take_profit_order_id)):
                    has_active_orders = True
                    break
            
            if not has_active_orders:
                # 检查是否还有实际仓位
                has_actual_positions = False
                for pos in position.positions:
                    account_index = getattr(pos, 'account_index', None)
                    if account_index is not None:
                        # 查询实际仓位
                        actual_positions = await self._get_exchange_positions(
                            account_index, getattr(pos, 'market_index', 0)
                        )
                        if actual_positions:
                            has_actual_positions = True
                            break
                
                if not has_actual_positions:
                    logger.info("检测到仓位已完全平仓",
                               position_id=position.id,
                               reason="所有止损止盈订单已执行且无实际仓位")
                    
                    # 可以在这里添加仓位完成的处理逻辑
                    # 例如：标记仓位状态、计算最终PnL、发送通知等
                    position.status = "COMPLETED"  # 如果有这个状态的话
                    position.updated_at = datetime.now()
                    
        except Exception as e:
            logger.error("检查仓位完成状态失败",
                        position_id=getattr(position, 'id', 'unknown'),
                        error=str(e))

    def get_system_status(self) -> SystemStatus:
        """Get current system status"""
        return SystemStatus(
            is_running=self.is_running,
            engine_status="running" if self.is_running else "stopped",
            active_positions=len(self.active_positions),
            total_accounts=len(self.account_manager.accounts),
            active_pairs=len([p for p in self.trading_pairs.values() if p.is_enabled]),
            last_trade_time=max(self.last_trade_times.values()) if self.last_trade_times else None,
            uptime=0.0  # Would calculate actual uptime
        )
    
    def _should_sync_orders_now(self, position: HedgePosition) -> Optional[str]:
        """
        智能判断是否需要同步订单状态
        返回同步原因，如果不需要同步则返回None
        """
        try:
            from datetime import datetime, timedelta
            
            now = datetime.now()
            
            # 检查是否是仓位创建后的关键时间点
            if hasattr(position, 'created_at') and position.created_at:
                time_since_creation = now - position.created_at
                
                # 仓位创建后前10分钟内，每30秒检查一次
                if time_since_creation < timedelta(minutes=10):
                    last_sync = getattr(position, '_last_sync_time', None)
                    if not last_sync or (now - last_sync) > timedelta(seconds=30):
                        position._last_sync_time = now
                        return "新仓位高频监控"
                
                # 仓位创建后10-60分钟内，每2分钟检查一次
                elif time_since_creation < timedelta(hours=1):
                    last_sync = getattr(position, '_last_sync_time', None)
                    if not last_sync or (now - last_sync) > timedelta(minutes=2):
                        position._last_sync_time = now
                        return "仓位中期监控"
                
                # 1小时后，每5分钟检查一次
                else:
                    last_sync = getattr(position, '_last_sync_time', None)
                    if not last_sync or (now - last_sync) > timedelta(minutes=5):
                        position._last_sync_time = now
                        return "仓位长期监控"
            
            # 检查是否有未解决的止损止盈订单
            if hasattr(position, 'positions'):
                for pos in position.positions:
                    # 如果有止损止盈订单ID但状态未确认，需要同步
                    if (hasattr(pos, 'stop_loss_order_id') and pos.stop_loss_order_id and
                        not getattr(pos, '_stop_loss_confirmed', False)):
                        return "止损订单状态待确认"
                    
                    if (hasattr(pos, 'take_profit_order_id') and pos.take_profit_order_id and
                        not getattr(pos, '_take_profit_confirmed', False)):
                        return "止盈订单状态待确认"
            
            # 检查最近是否有价格大幅波动（可能触发止损止盈）
            # 这里可以添加价格波动检测逻辑
            
            return None  # 不需要同步
            
        except Exception as e:
            logger.warning("智能同步判断失败", error=str(e))
            return "判断失败，执行保守同步"

    async def _update_order_confirmation_status(self, position: HedgePosition, sync_results: list) -> None:
        """
        根据同步结果更新订单确认状态
        """
        try:
            if not hasattr(position, 'positions'):
                return
            
            # 遍历每个子仓位，更新确认状态
            for pos in position.positions:
                # 检查止损订单状态
                if hasattr(pos, 'stop_loss_order_id') and pos.stop_loss_order_id:
                    for result in sync_results:
                        if isinstance(result, str) and result in ['filled', 'completed', 'executed']:
                            pos._stop_loss_confirmed = True
                            logger.debug("标记止损订单为已确认", 
                                       order_id=pos.stop_loss_order_id,
                                       status=result)
                            break
                
                # 检查止盈订单状态
                if hasattr(pos, 'take_profit_order_id') and pos.take_profit_order_id:
                    for result in sync_results:
                        if isinstance(result, str) and result in ['filled', 'completed', 'executed']:
                            pos._take_profit_confirmed = True
                            logger.debug("标记止盈订单为已确认", 
                                       order_id=pos.take_profit_order_id,
                                       status=result)
                            break
            
        except Exception as e:
            logger.warning("更新订单确认状态失败", error=str(e))

    async def cleanup(self) -> None:
        """Cleanup all resources"""
        await self.stop()
        await self.account_manager.cleanup()
        await self.order_manager.cleanup()
        await self.risk_manager.cleanup()
        await self.websocket_manager.cleanup()
        logger.info("对冲交易引擎已清理")