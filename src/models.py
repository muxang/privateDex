"""
Lighter Hedge Trading System - Data Models
"""

from enum import Enum
from typing import Dict, List, Optional, Any, Union
from datetime import datetime
from pydantic import BaseModel, Field
from decimal import Decimal


class PositionStatus(str, Enum):
    """Position status enumeration"""
    OPENING = "opening"
    ACTIVE = "active"
    CLOSING = "closing"
    CLOSED = "closed"
    FAILED = "failed"
    PENDING = "pending"  # 等待人工确认
    PENDING_CLOSE = "pending_close"  # 平仓后等待验证


class OrderStatus(str, Enum):
    """Order status enumeration"""
    PENDING = "pending"
    FILLED = "filled"
    CANCELLED = "cancelled"
    FAILED = "failed"


class HedgeStrategy(str, Enum):
    """Hedge strategy types"""
    BALANCED = "balanced"
    ZERO_LOSS = "zero_loss"
    AGGRESSIVE = "aggressive"


class RiskLimits(BaseModel):
    """Risk management limits"""
    max_daily_loss: Decimal
    max_position_size: Decimal
    max_slippage: Decimal
    min_balance: Decimal


class AccountConfig(BaseModel):
    """Account configuration"""
    index: int
    l1_address: str
    private_key: str
    api_key_index: int
    is_active: bool = True
    max_daily_trades: int = 100
    risk_limits: RiskLimits


class PriceConditions(BaseModel):
    """Price-based trading conditions"""
    max_spread_percent: Decimal = Decimal("0.01")
    max_volatility_percent: Decimal = Decimal("0.20")
    min_liquidity_depth: Decimal = Decimal("1000")


class LeverageConfig(BaseModel):
    """Leverage configuration for Lighter Protocol markets"""
    max_leverage: int
    initial_margin_requirement: Decimal  # IMR
    maintenance_margin_requirement: Decimal  # MMR
    close_out_margin_requirement: Decimal  # CMR
    
    @classmethod
    def from_market_type(cls, market_type: str) -> "LeverageConfig":
        """Create leverage config based on market type"""
        leverage_configs = {
            "BTC": cls(
                max_leverage=50,
                initial_margin_requirement=Decimal("0.02"),     # 2%
                maintenance_margin_requirement=Decimal("0.012"), # 1.2%
                close_out_margin_requirement=Decimal("0.008")   # 0.8%
            ),
            "ETH": cls(
                max_leverage=50,
                initial_margin_requirement=Decimal("0.02"),     # 2%
                maintenance_margin_requirement=Decimal("0.012"), # 1.2%
                close_out_margin_requirement=Decimal("0.008")   # 0.8%
            ),
            "SOL": cls(
                max_leverage=15,
                initial_margin_requirement=Decimal("0.0666"),   # 6.66%
                maintenance_margin_requirement=Decimal("0.0399"), # 3.99%
                close_out_margin_requirement=Decimal("0.0266")  # 2.66%
            ),
            "ALT": cls(  # For DOGE, PEPE, WIF, WLD, XRP, LINK, AVAX, NEAR, DOT
                max_leverage=8,
                initial_margin_requirement=Decimal("0.125"),    # 12.5%
                maintenance_margin_requirement=Decimal("0.075"), # 7.5%
                close_out_margin_requirement=Decimal("0.05")    # 5%
            ),
            "TON": cls(
                max_leverage=5,
                initial_margin_requirement=Decimal("0.20"),     # 20%
                maintenance_margin_requirement=Decimal("0.12"), # 12%
                close_out_margin_requirement=Decimal("0.08")    # 8%
            ),
            "TAO": cls(
                max_leverage=3,
                initial_margin_requirement=Decimal("0.3333"),   # 33.33%
                maintenance_margin_requirement=Decimal("0.20"), # 20%
                close_out_margin_requirement=Decimal("0.1333")  # 13.33%
            )
        }
        return leverage_configs.get(market_type.upper(), leverage_configs["ALT"])


class TradingPairConfig(BaseModel):
    """Trading pair configuration"""
    id: str
    name: str
    market_index: int
    is_enabled: bool = True
    take_profit_bps: int = 100  # 保留旧字段以兼容
    stop_loss_bps: int = 50    # 保留旧字段以兼容
    stop_take_distance: Union[str, float] = "auto"  # 止损止盈距离: "auto" 或 数字百分比
    max_positions: int = 3
    cooldown_minutes: int = 10
    account_addresses: List[str]
    hedge_strategy: HedgeStrategy = HedgeStrategy.BALANCED
    risk_limits: RiskLimits
    price_conditions: PriceConditions
    leverage_config: Optional["LeverageConfig"] = None
    leverage: int = 1  # Default 1x (no leverage)


class OrderInfo(BaseModel):
    """Order information"""
    id: str
    account_index: int
    market_index: int
    order_type: str
    side: str  # "buy" or "sell"
    amount: Decimal
    price: Optional[Decimal] = None
    trigger_price: Optional[Decimal] = None  # For stop loss and take profit orders
    status: OrderStatus
    created_at: datetime
    filled_at: Optional[datetime] = None
    filled_amount: Decimal = Decimal("0")
    filled_price: Optional[Decimal] = None
    cancelled_at: Optional[datetime] = None
    sdk_order_id: Optional[str] = None  # Lighter SDK order ID
    metadata: Dict[str, Any] = Field(default_factory=dict)


class Position(BaseModel):
    """Individual position"""
    id: str
    account_index: int
    market_index: int
    side: str  # "long" or "short"
    size: Decimal
    entry_price: Decimal
    current_price: Optional[Decimal] = None
    unrealized_pnl: Decimal = Decimal("0")
    leverage: int = 1  # Position leverage
    margin_used: Decimal = Decimal("0")  # Actual margin used for this position
    liquidation_price: Optional[Decimal] = None  # Calculated liquidation price
    margin_ratio: Optional[Decimal] = None  # Current margin ratio
    created_at: datetime
    updated_at: datetime


class HedgePosition(BaseModel):
    """Hedge position combining multiple individual positions"""
    id: str
    pair_id: str
    status: PositionStatus
    strategy: HedgeStrategy
    positions: List[Position] = Field(default_factory=list)
    total_pnl: Decimal = Decimal("0")
    created_at: datetime
    updated_at: datetime
    closed_at: Optional[datetime] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


class MarketData(BaseModel):
    """Market data structure"""
    market_index: int
    price: Decimal
    bid_price: Optional[Decimal] = None
    ask_price: Optional[Decimal] = None
    volume_24h: Optional[Decimal] = None
    price_change_24h: Optional[Decimal] = None
    timestamp: datetime


class OrderBook(BaseModel):
    """Order book data"""
    market_index: int
    bids: List[Dict[str, Decimal]] = Field(default_factory=list)
    asks: List[Dict[str, Decimal]] = Field(default_factory=list)
    timestamp: datetime


class Account(BaseModel):
    """Account information"""
    index: int
    l1_address: str
    balance: Decimal
    available_balance: Decimal
    positions: List[Position] = Field(default_factory=list)
    is_active: bool = True
    last_updated: datetime


class RiskEvent(BaseModel):
    """Risk management event"""
    service: str
    type: str
    account_index: Optional[int] = None
    pair_id: Optional[str] = None
    value: Decimal
    limit: Decimal
    severity: str
    action: str
    timestamp: datetime
    metadata: Dict[str, Any] = Field(default_factory=dict)


class TradingOpportunity(BaseModel):
    """Trading opportunity detection result"""
    pair_id: str
    is_valid: bool
    trigger_price: Decimal
    conditions_met: List[str] = Field(default_factory=list)
    conditions_failed: List[str] = Field(default_factory=list)
    confidence: Decimal = Decimal("0")
    timestamp: datetime
    metadata: Dict[str, Any] = Field(default_factory=dict)


class SystemStatus(BaseModel):
    """System status information"""
    is_running: bool
    engine_status: str
    active_positions: int
    total_accounts: int
    active_pairs: int
    last_trade_time: Optional[datetime] = None
    uptime: float
    errors_count: int = 0
    warnings_count: int = 0


class ConfigModel(BaseModel):
    """Complete system configuration model"""
    global_config: Dict[str, Any]
    web_config: Dict[str, Any]
    risk_management: Dict[str, Any]
    price_triggering: Dict[str, Any]
    trading_engine: Dict[str, Any]
    accounts: List[AccountConfig]
    trading_pairs: List[TradingPairConfig]