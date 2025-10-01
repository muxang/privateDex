"""
Configuration Manager for Lighter Hedge Trading System
"""

import yaml
import os
from typing import Dict, List, Optional, Any
from pathlib import Path
import structlog
from src.models import (
    ConfigModel, AccountConfig, TradingPairConfig, 
    RiskLimits, PriceConditions, HedgeStrategy
)
from decimal import Decimal

logger = structlog.get_logger()


class ConfigManager:
    """Manages system configuration loading and validation"""
    
    def __init__(self, config_path: Optional[str] = None):
        self.config_path = config_path or "config/config.yaml"
        self.config: Optional[ConfigModel] = None
        
    async def initialize(self) -> None:
        """Initialize configuration manager"""
        try:
            await self.load_config()
            await self.validate_config()
            logger.info("配置管理器初始化完成")
        except Exception as e:
            logger.error("配置管理器初始化失败", error=str(e))
            raise
    
    async def load_config(self) -> None:
        """Load configuration from YAML file"""
        try:
            config_file = Path(self.config_path)
            if not config_file.exists():
                raise FileNotFoundError(f"配置文件不存在: {self.config_path}")
            
            with open(config_file, 'r', encoding='utf-8') as f:
                raw_config = yaml.safe_load(f)
            
            # Parse accounts
            accounts = []
            for acc_data in raw_config.get('accounts', []):
                risk_limits = RiskLimits(
                    max_daily_loss=Decimal(str(acc_data['risk_limits']['max_daily_loss'])),
                    max_position_size=Decimal(str(acc_data['risk_limits']['max_position_size'])),
                    max_slippage=Decimal(str(acc_data['risk_limits']['max_slippage'])),
                    min_balance=Decimal(str(acc_data['risk_limits']['min_balance']))
                )
                
                account = AccountConfig(
                    index=acc_data['index'],
                    l1_address=acc_data['l1_address'],
                    private_key=acc_data['private_key'],
                    api_key_index=acc_data['api_key_index'],
                    is_active=acc_data.get('is_active', True),
                    max_daily_trades=acc_data.get('max_daily_trades', 100),
                    risk_limits=risk_limits
                )
                accounts.append(account)
            
            # Parse trading pairs
            trading_pairs = []
            for pair_data in raw_config.get('trading_pairs', []):
                risk_limits = RiskLimits(
                    max_daily_loss=Decimal(str(pair_data['risk_limits']['max_daily_loss'])),
                    max_position_size=Decimal(str(pair_data['risk_limits']['max_position_size'])),
                    max_slippage=Decimal(str(pair_data['risk_limits']['max_slippage'])),
                    min_balance=Decimal(str(pair_data['risk_limits']['min_balance']))
                )
                
                price_conditions = PriceConditions(
                    max_spread_percent=Decimal(str(pair_data.get('price_conditions', {}).get('max_spread_percent', '0.01'))),
                    max_volatility_percent=Decimal(str(pair_data.get('price_conditions', {}).get('max_volatility_percent', '0.20'))),
                    min_liquidity_depth=Decimal(str(pair_data.get('price_conditions', {}).get('min_liquidity_depth', '1000')))
                )
                
                trading_pair = TradingPairConfig(
                    id=pair_data['id'],
                    name=pair_data['name'],
                    market_index=pair_data['market_index'],
                    is_enabled=pair_data.get('is_enabled', True),
                    take_profit_bps=pair_data.get('take_profit_bps', 100),
                    stop_loss_bps=pair_data.get('stop_loss_bps', 50),
                    stop_take_distance=pair_data.get('stop_take_distance', 'auto'),
                    max_positions=pair_data.get('max_positions', 3),
                    cooldown_minutes=pair_data.get('cooldown_minutes', 10),
                    account_addresses=pair_data['account_addresses'],
                    hedge_strategy=HedgeStrategy(pair_data.get('hedge_strategy', 'balanced')),
                    risk_limits=risk_limits,
                    price_conditions=price_conditions,
                    leverage=pair_data.get('leverage', 1)  # 使用正确的字段名
                )
                trading_pairs.append(trading_pair)
            
            # Create complete config model
            self.config = ConfigModel(
                global_config=raw_config.get('global', {}),
                web_config=raw_config.get('web', {}),
                risk_management=raw_config.get('risk_management', {}),
                price_triggering=raw_config.get('price_triggering', {}),
                trading_engine=raw_config.get('trading_engine', {}),
                accounts=accounts,
                trading_pairs=trading_pairs
            )
            
            logger.info("配置文件加载完成", 
                       accounts_count=len(accounts),
                       trading_pairs_count=len(trading_pairs))
            
        except Exception as e:
            logger.error("配置文件加载失败", error=str(e))
            raise
    
    async def validate_config(self) -> None:
        """Validate configuration"""
        if not self.config:
            raise ValueError("配置未加载")
        
        errors = []
        
        # Validate accounts
        active_accounts = [acc for acc in self.config.accounts if acc.is_active]
        if len(active_accounts) < 2:
            errors.append("至少需要两个活跃账户进行对冲交易")
        
        # Validate trading pairs
        for pair in self.config.trading_pairs:
            if pair.is_enabled:
                # Check if accounts exist
                pair_accounts = self.get_pair_accounts(pair)
                if len(pair_accounts) < 2:
                    errors.append(f"交易对 {pair.id} 至少需要两个有效账户")
                
                # Check if accounts are active
                inactive_accounts = [acc for acc in pair_accounts if not acc.is_active]
                if inactive_accounts:
                    inactive_addresses = [acc.l1_address for acc in inactive_accounts]
                    errors.append(f"交易对 {pair.id} 引用的账户 {inactive_addresses} 未激活")
        
        if errors:
            error_msg = "; ".join(errors)
            logger.error("配置验证失败", errors=errors)
            raise ValueError(f"配置验证失败: {error_msg}")
        
        logger.info("配置验证通过")
    
    def get_global_config(self) -> Dict[str, Any]:
        """Get global configuration"""
        if not self.config:
            raise ValueError("配置未加载")
        return self.config.global_config
    
    def get_web_config(self) -> Dict[str, Any]:
        """Get web configuration"""
        if not self.config:
            raise ValueError("配置未加载")
        return self.config.web_config
    
    def get_risk_config(self) -> Dict[str, Any]:
        """Get risk management configuration"""
        if not self.config:
            raise ValueError("配置未加载")
        return self.config.risk_management
    
    def get_trading_engine_config(self) -> Dict[str, Any]:
        """Get trading engine configuration"""
        if not self.config:
            raise ValueError("配置未加载")
        return self.config.trading_engine
    
    def get_accounts(self) -> List[AccountConfig]:
        """Get all accounts"""
        if not self.config:
            raise ValueError("配置未加载")
        return self.config.accounts
    
    def get_active_accounts(self) -> List[AccountConfig]:
        """Get active accounts only"""
        return [acc for acc in self.get_accounts() if acc.is_active]
    
    def get_account_by_index(self, index: int) -> Optional[AccountConfig]:
        """Get account by index"""
        for account in self.get_accounts():
            if account.index == index:
                return account
        return None
    
    def get_account_by_address(self, address: str) -> Optional[AccountConfig]:
        """Get account by L1 address"""
        for account in self.get_accounts():
            if account.l1_address.lower() == address.lower():
                return account
        return None
    
    def get_trading_pairs(self) -> List[TradingPairConfig]:
        """Get all trading pairs"""
        if not self.config:
            raise ValueError("配置未加载")
        return self.config.trading_pairs
    
    def get_active_pairs(self) -> List[TradingPairConfig]:
        """Get enabled trading pairs only"""
        return [pair for pair in self.get_trading_pairs() if pair.is_enabled]
    
    def get_pair_by_id(self, pair_id: str) -> Optional[TradingPairConfig]:
        """Get trading pair by ID"""
        for pair in self.get_trading_pairs():
            if pair.id == pair_id:
                return pair
        return None
    
    def get_pair_accounts(self, pair: TradingPairConfig) -> List[AccountConfig]:
        """Get accounts associated with a trading pair"""
        accounts = []
        for address in pair.account_addresses:
            account = self.get_account_by_address(address)
            if account:
                accounts.append(account)
        return accounts
    
    async def reload_config(self) -> None:
        """Reload configuration from file"""
        await self.load_config()
        await self.validate_config()
        logger.info("配置已重新加载")
    
    def get_api_url(self) -> str:
        """Get API URL"""
        return self.get_global_config().get('api_url', 'https://mainnet.zklighter.elliot.ai')
    
    def get_ws_url(self) -> str:
        """Get WebSocket URL"""
        return self.get_global_config().get('ws_url', 'wss://mainnet.zklighter.elliot.ai/ws')
    
    def get_monitoring_interval(self) -> int:
        """Get monitoring interval in seconds"""
        return self.get_global_config().get('monitoring_interval', 5)