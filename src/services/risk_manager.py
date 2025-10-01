"""
Risk Manager for Lighter Hedge Trading System
"""

import asyncio
from typing import Dict, List, Optional, Callable
from datetime import datetime, timedelta
from decimal import Decimal
import structlog
from dataclasses import dataclass

from src.models import RiskEvent, Account, TradingPairConfig, HedgePosition
from src.config.config_manager import ConfigManager
from src.services.account_manager import AccountManager

logger = structlog.get_logger()


@dataclass
class RiskCheckResult:
    """Risk check result"""
    allowed: bool
    reason: Optional[str] = None
    severity: str = "info"
    action: Optional[str] = None


class RiskManager:
    """Manages risk controls and limits"""
    
    def __init__(self, config_manager: ConfigManager, account_manager: AccountManager):
        self.config_manager = config_manager
        self.account_manager = account_manager
        self.daily_losses: Dict[int, Decimal] = {}  # Account daily losses
        self.pair_daily_losses: Dict[str, Decimal] = {}  # Pair daily losses
        self.risk_events: List[RiskEvent] = []
        self.emergency_stop_active: bool = False
        self.risk_event_handlers: List[Callable[[RiskEvent], None]] = []
        self._monitoring_task: Optional[asyncio.Task] = None
        
    async def initialize(self) -> None:
        """Initialize risk manager"""
        try:
            # Reset daily counters
            self.reset_daily_counters()
            
            # Start risk monitoring
            await self.start_monitoring()
            
            logger.info("风险管理器初始化完成")
            
        except Exception as e:
            logger.error("风险管理器初始化失败", error=str(e))
            raise
    
    def reset_daily_counters(self) -> None:
        """Reset daily counters"""
        self.daily_losses.clear()
        self.pair_daily_losses.clear()
        logger.info("日风险计数器已重置")
    
    def add_risk_event_handler(self, handler: Callable[[RiskEvent], None]) -> None:
        """Add a risk event handler"""
        self.risk_event_handlers.append(handler)
    
    async def check_open_position_risk(
        self,
        pair_config: TradingPairConfig,
        base_amount: Decimal
    ) -> RiskCheckResult:
        """Check if opening a position is allowed"""
        try:
            # Check emergency stop
            if self.emergency_stop_active:
                return RiskCheckResult(
                    allowed=False,
                    reason="紧急停止已激活",
                    severity="critical",
                    action="wait"
                )
            
            # Check global daily loss limit
            global_config = self.config_manager.get_risk_config()
            global_max_loss = Decimal(str(global_config.get('global_max_daily_loss', 5000)))
            
            total_daily_loss = sum(self.daily_losses.values())
            if total_daily_loss >= global_max_loss:
                await self._trigger_risk_event(
                    service="risk_manager",
                    event_type="global_daily_loss_limit",
                    value=total_daily_loss,
                    limit=global_max_loss,
                    severity="critical",
                    action="emergency_stop"
                )
                return RiskCheckResult(
                    allowed=False,
                    reason=f"全局日损失限制: {total_daily_loss} >= {global_max_loss}",
                    severity="critical",
                    action="emergency_stop"
                )
            
            # Check pair daily loss limit
            pair_daily_loss = self.pair_daily_losses.get(pair_config.id, Decimal('0'))
            if pair_daily_loss >= pair_config.risk_limits.max_daily_loss:
                return RiskCheckResult(
                    allowed=False,
                    reason=f"交易对日损失限制: {pair_daily_loss} >= {pair_config.risk_limits.max_daily_loss}",
                    severity="warning",
                    action="skip_pair"
                )
            
            # Check position size limit
            if base_amount > pair_config.risk_limits.max_position_size:
                return RiskCheckResult(
                    allowed=False,
                    reason=f"仓位大小超限: {base_amount} > {pair_config.risk_limits.max_position_size}",
                    severity="warning",
                    action="reduce_size"
                )
            
            # Check account balances
            pair_accounts = self.config_manager.get_pair_accounts(pair_config)
            for account_config in pair_accounts:
                account = self.account_manager.get_account(account_config.index)
                if not account:
                    return RiskCheckResult(
                        allowed=False,
                        reason=f"账户 {account_config.index} 数据不可用",
                        severity="warning",
                        action="wait"
                    )
                
                # Check minimum balance
                if account.available_balance < account_config.risk_limits.min_balance:
                    await self._trigger_risk_event(
                        service="risk_manager",
                        event_type="balance_low",
                        account_index=account_config.index,
                        value=account.available_balance,
                        limit=account_config.risk_limits.min_balance,
                        severity="warning",
                        action="skip_account"
                    )
                    return RiskCheckResult(
                        allowed=False,
                        reason=f"账户 {account_config.index} 余额不足: {account.available_balance} < {account_config.risk_limits.min_balance}",
                        severity="warning",
                        action="skip_account"
                    )
                
                # Check account daily loss
                account_daily_loss = self.daily_losses.get(account_config.index, Decimal('0'))
                if account_daily_loss >= account_config.risk_limits.max_daily_loss:
                    return RiskCheckResult(
                        allowed=False,
                        reason=f"账户 {account_config.index} 日损失超限: {account_daily_loss} >= {account_config.risk_limits.max_daily_loss}",
                        severity="warning",
                        action="skip_account"
                    )
            
            return RiskCheckResult(allowed=True)
            
        except Exception as e:
            logger.error("风险检查失败", error=str(e))
            return RiskCheckResult(
                allowed=False,
                reason=f"风险检查出错: {str(e)}",
                severity="error",
                action="wait"
            )
    
    async def check_close_position_risk(
        self,
        position: HedgePosition
    ) -> RiskCheckResult:
        """Check if closing a position is allowed"""
        # Generally allow position closing for risk management
        return RiskCheckResult(allowed=True)
    
    async def record_trade_pnl(
        self,
        account_index: int,
        pair_id: str,
        pnl: Decimal
    ) -> None:
        """Record trade P&L for risk tracking"""
        try:
            # Update account daily loss (only track losses)
            if pnl < 0:
                current_loss = self.daily_losses.get(account_index, Decimal('0'))
                self.daily_losses[account_index] = current_loss + abs(pnl)
                
                # Update pair daily loss
                current_pair_loss = self.pair_daily_losses.get(pair_id, Decimal('0'))
                self.pair_daily_losses[pair_id] = current_pair_loss + abs(pnl)
            
            # Check if limits are approached
            await self._check_loss_limits(account_index, pair_id)
            
            logger.debug("记录交易盈亏",
                        account_index=account_index,
                        pair_id=pair_id,
                        pnl=float(pnl))
            
        except Exception as e:
            logger.error("记录交易盈亏失败", error=str(e))
    
    async def _check_loss_limits(self, account_index: int, pair_id: str) -> None:
        """Check if loss limits are being approached"""
        # Check account loss limit
        account_config = self.config_manager.get_account_by_index(account_index)
        if account_config:
            account_loss = self.daily_losses.get(account_index, Decimal('0'))
            account_limit = account_config.risk_limits.max_daily_loss
            
            # Warning at 80% of limit
            if account_loss >= account_limit * Decimal('0.8'):
                await self._trigger_risk_event(
                    service="risk_manager",
                    event_type="account_loss_warning",
                    account_index=account_index,
                    value=account_loss,
                    limit=account_limit,
                    severity="warning",
                    action="monitor"
                )
        
        # Check pair loss limit
        pair_config = self.config_manager.get_pair_by_id(pair_id)
        if pair_config:
            pair_loss = self.pair_daily_losses.get(pair_id, Decimal('0'))
            pair_limit = pair_config.risk_limits.max_daily_loss
            
            # Warning at 80% of limit
            if pair_loss >= pair_limit * Decimal('0.8'):
                await self._trigger_risk_event(
                    service="risk_manager",
                    event_type="pair_loss_warning",
                    pair_id=pair_id,
                    value=pair_loss,
                    limit=pair_limit,
                    severity="warning",
                    action="monitor"
                )
    
    async def _trigger_risk_event(
        self,
        service: str,
        event_type: str,
        value: Decimal,
        limit: Decimal,
        severity: str,
        action: str,
        account_index: Optional[int] = None,
        pair_id: Optional[str] = None
    ) -> None:
        """Trigger a risk event"""
        event = RiskEvent(
            service=service,
            type=event_type,
            account_index=account_index,
            pair_id=pair_id,
            value=value,
            limit=limit,
            severity=severity,
            action=action,
            timestamp=datetime.now()
        )
        
        self.risk_events.append(event)
        
        # Execute action
        if action == "emergency_stop":
            await self.activate_emergency_stop()
        
        # Notify handlers
        for handler in self.risk_event_handlers:
            try:
                handler(event)
            except Exception as e:
                logger.error("风险事件处理器执行失败", error=str(e))
        
        logger.warning("风险事件触发",
                      service=service,
                      type=event_type,
                      account_index=account_index,
                      pair_id=pair_id,
                      value=float(value),
                      limit=float(limit),
                      severity=severity,
                      action=action)
    
    async def activate_emergency_stop(self) -> None:
        """Activate emergency stop"""
        self.emergency_stop_active = True
        logger.critical("紧急停止已激活")
    
    async def deactivate_emergency_stop(self) -> None:
        """Deactivate emergency stop"""
        self.emergency_stop_active = False
        logger.info("紧急停止已解除")
    
    def is_emergency_stop_active(self) -> bool:
        """Check if emergency stop is active"""
        return self.emergency_stop_active
    
    async def start_monitoring(self) -> None:
        """Start risk monitoring"""
        if self._monitoring_task:
            self._monitoring_task.cancel()
        
        risk_config = self.config_manager.get_risk_config()
        interval = risk_config.get('risk_check_interval', 10)
        
        self._monitoring_task = asyncio.create_task(self._monitoring_loop(interval))
        logger.info("风险监控已启动", check_interval=interval)
    
    async def stop_monitoring(self) -> None:
        """Stop risk monitoring"""
        if self._monitoring_task:
            self._monitoring_task.cancel()
            self._monitoring_task = None
        logger.info("风险监控已停止")
    
    async def _monitoring_loop(self, interval: int) -> None:
        """Risk monitoring loop"""
        while True:
            try:
                await asyncio.sleep(interval)
                await self._perform_risk_checks()
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("风险监控循环出错", error=str(e))
                await asyncio.sleep(interval)
    
    async def _perform_risk_checks(self) -> None:
        """Perform periodic risk checks"""
        try:
            # Check account balances
            active_accounts = self.config_manager.get_active_accounts()
            for account_config in active_accounts:
                account = self.account_manager.get_account(account_config.index)
                if account and account.available_balance < account_config.risk_limits.min_balance:
                    await self._trigger_risk_event(
                        service="risk_monitor",
                        event_type="balance_low",
                        account_index=account_config.index,
                        value=account.available_balance,
                        limit=account_config.risk_limits.min_balance,
                        severity="warning",
                        action="monitor"
                    )
            
        except Exception as e:
            logger.error("定期风险检查失败", error=str(e))
    
    def get_risk_summary(self) -> Dict:
        """Get risk summary"""
        return {
            "emergency_stop_active": self.emergency_stop_active,
            "total_daily_loss": float(sum(self.daily_losses.values())),
            "account_losses": {k: float(v) for k, v in self.daily_losses.items()},
            "pair_losses": {k: float(v) for k, v in self.pair_daily_losses.items()},
            "recent_events_count": len([e for e in self.risk_events 
                                      if e.timestamp > datetime.now() - timedelta(hours=1)])
        }
    
    async def cleanup(self) -> None:
        """Cleanup resources"""
        await self.stop_monitoring()
        logger.info("风险管理器已清理")