"""
Account Manager for Lighter Hedge Trading System
"""

import asyncio
from typing import Dict, List, Optional
from datetime import datetime, timedelta
from decimal import Decimal
import structlog

import lighter
from src.models import Account, AccountConfig, Position
from src.config.config_manager import ConfigManager
from src.core.lighter_client_factory import get_client_factory

logger = structlog.get_logger()


class AccountManager:
    """Manages account information and operations"""
    
    def __init__(self, config_manager: ConfigManager):
        self.config_manager = config_manager
        self.accounts: Dict[int, Account] = {}
        self.client_factory = get_client_factory(config_manager)
        self.account_api: Optional[lighter.AccountApi] = None
        self.daily_trade_counts: Dict[int, int] = {}
        self.last_trade_time: Dict[int, datetime] = {}
        self.account_cooldowns: Dict[int, datetime] = {}
        self._monitoring_task: Optional[asyncio.Task] = None
    
    async def _get_account_index_by_address(self, l1_address: str) -> int:
        """通过L1地址获取账户index"""
        try:
            # 方法1: 使用account API通过l1_address查询
            account_response = await self.account_api.account(
                by="l1_address", 
                value=l1_address
            )
            
            if account_response and account_response.accounts:
                account_data = account_response.accounts[0]
                if hasattr(account_data, 'account_index'):
                    account_index = int(account_data.account_index)
                    logger.info("通过account API获取到账户index", 
                               l1_address=l1_address, 
                               account_index=account_index)
                    return account_index
                elif hasattr(account_data, 'index'):
                    account_index = int(account_data.index)
                    logger.info("通过account API获取到账户index(使用index字段)", 
                               l1_address=l1_address, 
                               account_index=account_index)
                    return account_index
            
            # 方法2: 使用accounts_by_l1_address API（如果可用）
            if hasattr(self.account_api, 'accounts_by_l1_address'):
                try:
                    accounts_response = await self.account_api.accounts_by_l1_address(
                        l1_address=l1_address
                    )
                    if accounts_response and hasattr(accounts_response, 'accounts') and accounts_response.accounts:
                        account_data = accounts_response.accounts[0]
                        if hasattr(account_data, 'account_index'):
                            account_index = int(account_data.account_index)
                            logger.info("通过accounts_by_l1_address API获取到账户index", 
                                       l1_address=l1_address, 
                                       account_index=account_index)
                            return account_index
                        elif hasattr(account_data, 'index'):
                            account_index = int(account_data.index)
                            logger.info("通过accounts_by_l1_address API获取到账户index(使用index字段)", 
                                       l1_address=l1_address, 
                                       account_index=account_index)
                            return account_index
                except Exception as e:
                    logger.debug("accounts_by_l1_address方法调用失败", error=str(e))
            
            logger.warning("无法通过L1地址获取账户index", l1_address=l1_address)
            return 0
            
        except Exception as e:
            logger.error("通过L1地址获取账户index时发生错误", 
                        l1_address=l1_address, 
                        error=str(e))
            return 0
    
    async def _update_account_index_in_config(self, l1_address: str, new_index: int) -> None:
        """更新配置文件中的账户index（可选功能）"""
        try:
            import yaml
            from pathlib import Path
            
            config_path = Path(self.config_manager.config_path)
            if not config_path.exists():
                logger.warning("配置文件不存在，无法更新index", config_path=str(config_path))
                return
            
            # 读取当前配置
            with open(config_path, 'r', encoding='utf-8') as f:
                config_data = yaml.safe_load(f)
            
            # 查找并更新对应账户的index
            updated = False
            if 'accounts' in config_data:
                for account in config_data['accounts']:
                    if account.get('l1_address', '').lower() == l1_address.lower():
                        old_index = account.get('index', 0)
                        account['index'] = new_index
                        updated = True
                        logger.info("配置文件中的账户index已更新", 
                                   l1_address=l1_address,
                                   old_index=old_index,
                                   new_index=new_index)
                        break
            
            # 保存更新后的配置
            if updated:
                # 创建备份
                backup_path = config_path.with_suffix('.yaml.backup')
                config_path.replace(backup_path)
                
                # 写入新配置
                with open(config_path, 'w', encoding='utf-8') as f:
                    yaml.dump(config_data, f, default_flow_style=False, 
                             allow_unicode=True, sort_keys=False)
                
                logger.info("配置文件已更新并创建备份", 
                           config_path=str(config_path),
                           backup_path=str(backup_path))
            else:
                logger.warning("未找到匹配的账户地址，无法更新配置", l1_address=l1_address)
                
        except Exception as e:
            logger.error("更新配置文件中的账户index失败", 
                        l1_address=l1_address, 
                        new_index=new_index,
                        error=str(e))
        
    async def initialize(self) -> None:
        """Initialize account manager"""
        try:
            # Use client factory for API initialization
            self.account_api = self.client_factory.get_account_api()
            
            # Load all accounts
            await self.load_all_accounts()
            
            # Start monitoring
            await self.start_monitoring()
            
            logger.info("账户管理器初始化完成", account_count=len(self.accounts))
            
        except Exception as e:
            logger.error("账户管理器初始化失败", error=str(e))
            raise
    
    async def load_all_accounts(self) -> None:
        """Load all configured accounts"""
        account_configs = self.config_manager.get_active_accounts()
        
        for config in account_configs:
            try:
                await self.load_account(config)
            except Exception as e:
                logger.error("加载账户失败", 
                           account_index=config.index,
                           error=str(e))
    
    async def load_account(self, config: AccountConfig) -> None:
        """Load a single account"""
        try:
            # 如果配置的index为0，尝试通过L1地址自动获取正确的index
            actual_index = config.index
            if config.index == 0:
                logger.info("检测到账户index为0，尝试通过L1地址自动获取正确的index", 
                           l1_address=config.l1_address)
                actual_index = await self._get_account_index_by_address(config.l1_address)
                if actual_index != 0:
                    logger.info("成功获取账户index", 
                               l1_address=config.l1_address,
                               old_index=config.index,
                               new_index=actual_index)
                    # 更新配置中的index
                    config.index = actual_index
                    # 可选：保存更新后的index到配置文件中
                    await self._update_account_index_in_config(config.l1_address, actual_index)
                else:
                    logger.warning("无法通过L1地址获取有效的账户index", 
                                  l1_address=config.l1_address)
            
            # Get account information from API
            account_response = await self.account_api.account(
                by="index", 
                value=str(actual_index)
            )
            
            if not account_response or not account_response.accounts:
                # 如果通过index查询失败，尝试通过L1地址查询
                if actual_index != 0:
                    logger.warning(f"通过index {actual_index} 查询失败，尝试通过L1地址查询")
                    account_response = await self.account_api.account(
                        by="l1_address", 
                        value=config.l1_address
                    )
                
                if not account_response or not account_response.accounts:
                    raise ValueError(f"无法获取账户 {config.l1_address} 的数据")
            
            # Get the first account from the response
            account_data = account_response.accounts[0]
            
            # Parse account data from DetailedAccount object
            if hasattr(account_data, 'collateral'):
                balance = Decimal(str(account_data.collateral))
            else:
                balance = Decimal('0')
            
            if hasattr(account_data, 'available_balance'):
                available_balance = Decimal(str(account_data.available_balance))
            else:
                available_balance = balance
            
            # Get positions - handle official API response structure
            positions = []
            if hasattr(account_data, 'positions') and account_data.positions:
                for pos_data in account_data.positions:
                    # 详细调试位置数据字段
                    pos_attrs = [attr for attr in dir(pos_data) if not attr.startswith('_')]
                    logger.info("🔍 仓位数据字段分析",
                               account_index=config.index,
                               pos_attrs=pos_attrs[:15],  # 限制输出长度
                               pos_type=type(pos_data).__name__)
                    
                    # 尝试多种可能的市场索引字段名
                    market_index = 0  # 默认值
                    for field_name in ['market_index', 'market_id', 'market', 'marketIndex']:
                        if hasattr(pos_data, field_name):
                            field_value = getattr(pos_data, field_name)
                            logger.info("🎯 找到市场字段",
                                       field_name=field_name,
                                       field_value=field_value,
                                       field_type=type(field_value).__name__)
                            if field_value is not None:
                                market_index = int(field_value)
                                break
                    
                    # 检查所有可能的仓位大小字段
                    size_value = None
                    size_field_used = None
                    
                    # 先显示所有可能的大小字段值
                    size_fields_analysis = {}
                    for size_field in ['position', 'position_value', 'sign', 'allocated_margin', 'size', 'amount']:
                        if hasattr(pos_data, size_field):
                            potential_size = getattr(pos_data, size_field)
                            size_fields_analysis[size_field] = {
                                'value': potential_size,
                                'type': type(potential_size).__name__
                            }
                    
                    logger.info("🔍 所有大小字段检查",
                               account_index=config.index,
                               size_fields=size_fields_analysis)
                    
                    # 根据官方文档，Lighter Protocol使用position字段作为仓位大小
                    size_value = None
                    size_field_used = None
                    position_sign = None
                    
                    # 获取position字段（主要的仓位大小）
                    if hasattr(pos_data, 'position'):
                        position_str = getattr(pos_data, 'position')
                        if position_str is not None:
                            try:
                                size_value = float(position_str) if isinstance(position_str, str) else position_str
                                size_field_used = 'position'
                            except (ValueError, TypeError):
                                size_value = 0
                                size_field_used = 'position(invalid)'
                    
                    # 获取sign字段（仓位方向：1=Long, -1=Short）
                    if hasattr(pos_data, 'sign'):
                        position_sign = getattr(pos_data, 'sign')
                    
                    # 如果没有position字段，尝试其他可能的字段
                    if size_value is None or size_value == 0:
                        for size_field in ['position_value', 'allocated_margin', 'size', 'amount']:
                            if hasattr(pos_data, size_field):
                                potential_size = getattr(pos_data, size_field)
                                if potential_size is not None:
                                    try:
                                        potential_value = float(potential_size) if isinstance(potential_size, str) else potential_size
                                        if potential_value != 0:
                                            size_value = potential_value
                                            size_field_used = size_field
                                            logger.info("🎯 选择备用大小字段",
                                                       field_name=size_field,
                                                       field_value=potential_value)
                                            break
                                    except (ValueError, TypeError):
                                        continue
                    
                    # 确保size_value不为None
                    if size_value is None:
                        size_value = 0
                        size_field_used = 'default'
                    
                    # 根据sign确定仓位方向
                    if position_sign == 1:
                        position_side = 'long'
                    elif position_sign == -1:
                        position_side = 'short'
                    else:
                        position_side = getattr(pos_data, 'side', 'unknown')
                    
                    position = Position(
                        id=getattr(pos_data, 'id', ''),
                        account_index=config.index,
                        market_index=market_index,
                        side=position_side,
                        size=Decimal(str(size_value)),
                        entry_price=Decimal(str(getattr(pos_data, 'avg_entry_price', '0'))),
                        current_price=Decimal(str(getattr(pos_data, 'current_price', '0'))),
                        unrealized_pnl=Decimal(str(getattr(pos_data, 'unrealized_pnl', '0'))),
                        created_at=datetime.now(),
                        updated_at=datetime.now()
                    )
                    
                    logger.info("📍 解析仓位信息",
                               account_index=config.index,
                               position_id=position.id,
                               market_index=position.market_index,
                               side=position.side,
                               size=float(position.size),
                               size_field_used=size_field_used,
                               position_sign=position_sign)
                    
                    positions.append(position)
            
            # Create account object
            account = Account(
                index=config.index,
                l1_address=config.l1_address,
                balance=balance,
                available_balance=available_balance,
                positions=positions,
                is_active=config.is_active,
                last_updated=datetime.now()
            )
            
            self.accounts[config.index] = account
            
            logger.info("账户已加载",
                       account_index=config.index,
                       balance=float(balance),
                       l1_address=config.l1_address)
            
        except Exception as e:
            logger.error("加载账户失败",
                        account_index=config.index,
                        error=str(e))
            raise
    
    async def refresh_account(self, account_index: int) -> None:
        """Refresh account data"""
        config = self.config_manager.get_account_by_index(account_index)
        if config:
            await self.load_account(config)
    
    async def refresh_all_accounts(self) -> None:
        """Refresh all accounts"""
        await self.load_all_accounts()
    
    def get_account(self, account_index: int) -> Optional[Account]:
        """Get account by index"""
        return self.accounts.get(account_index)
    
    def get_account_balance(self, account_index: int) -> Decimal:
        """Get account balance"""
        account = self.get_account(account_index)
        return account.balance if account else Decimal('0')
    
    def get_available_balance(self, account_index: int) -> Decimal:
        """Get available balance"""
        account = self.get_account(account_index)
        return account.available_balance if account else Decimal('0')
    
    def is_account_active(self, account_index: int) -> bool:
        """Check if account is active"""
        account = self.get_account(account_index)
        return account.is_active if account else False
    
    def get_account_positions(self, account_index: int) -> List[Position]:
        """Get account positions"""
        account = self.get_account(account_index)
        return account.positions if account else []
    
    def has_sufficient_balance(self, account_index: int, required_amount: Decimal) -> bool:
        """Check if account has sufficient balance"""
        available = self.get_available_balance(account_index)
        return available >= required_amount
    
    def is_account_in_cooldown(self, account_index: int) -> bool:
        """Check if account is in cooldown period"""
        if account_index not in self.account_cooldowns:
            return False
        
        cooldown_until = self.account_cooldowns[account_index]
        return datetime.now() < cooldown_until
    
    def set_account_cooldown(self, account_index: int, minutes: int) -> None:
        """Set account cooldown period"""
        cooldown_until = datetime.now() + timedelta(minutes=minutes)
        self.account_cooldowns[account_index] = cooldown_until
        
        logger.debug("设置账户冷却期",
                    account_index=account_index,
                    cooldown_minutes=minutes,
                    cooldown_until=cooldown_until.isoformat())
    
    def get_daily_trade_count(self, account_index: int) -> int:
        """Get daily trade count for account"""
        return self.daily_trade_counts.get(account_index, 0)
    
    def increment_daily_trade_count(self, account_index: int) -> None:
        """Increment daily trade count"""
        self.daily_trade_counts[account_index] = self.get_daily_trade_count(account_index) + 1
        self.last_trade_time[account_index] = datetime.now()
        
        logger.debug("增加日交易计数",
                    account_index=account_index,
                    daily_count=self.daily_trade_counts[account_index])
    
    def reset_daily_trade_counts(self) -> None:
        """Reset daily trade counts (called daily)"""
        self.daily_trade_counts.clear()
        logger.info("日交易计数已重置")
    
    async def start_monitoring(self) -> None:
        """Start account monitoring"""
        if self._monitoring_task:
            self._monitoring_task.cancel()
        
        self._monitoring_task = asyncio.create_task(self._monitoring_loop())
        logger.info("账户监控已启动")
    
    async def stop_monitoring(self) -> None:
        """Stop account monitoring"""
        if self._monitoring_task:
            self._monitoring_task.cancel()
            self._monitoring_task = None
        logger.info("账户监控已停止")
    
    async def _monitoring_loop(self) -> None:
        """Account monitoring loop"""
        while True:
            try:
                await asyncio.sleep(30)  # Refresh every 30 seconds
                await self.refresh_all_accounts()
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("账户监控循环出错", error=str(e))
                await asyncio.sleep(10)  # Wait before retrying
    
    def get_available_accounts_for_trading(self, required_balance: Decimal) -> List[int]:
        """Get accounts available for trading"""
        available_accounts = []
        
        for account_index, account in self.accounts.items():
            if not account.is_active:
                continue
            
            if self.is_account_in_cooldown(account_index):
                continue
            
            if not self.has_sufficient_balance(account_index, required_balance):
                continue
            
            # Check daily trade limits
            config = self.config_manager.get_account_by_index(account_index)
            if config:
                daily_count = self.get_daily_trade_count(account_index)
                if daily_count >= config.max_daily_trades:
                    continue
            
            available_accounts.append(account_index)
        
        return available_accounts
    
    async def cleanup(self) -> None:
        """Cleanup resources"""
        await self.stop_monitoring()
        
        if self.account_api and hasattr(self.account_api, 'api_client'):
            # Close API client if it has a close method
            if hasattr(self.account_api.api_client, 'close'):
                await self.account_api.api_client.close()
        
        logger.info("账户管理器已清理")
    
    def update_account_data(self, account_index: int, account_data) -> None:
        """更新账户数据（来自WebSocket）"""
        try:
            if account_index in self.accounts:
                account = self.accounts[account_index]
                
                # Handle both dict and object formats from WebSocket
                if isinstance(account_data, dict):
                    # Dictionary format
                    if 'balance' in account_data:
                        account.balance = Decimal(str(account_data['balance']))
                    
                    if 'available_balance' in account_data:
                        account.available_balance = Decimal(str(account_data['available_balance']))
                else:
                    # Object format from official API
                    if hasattr(account_data, 'balance'):
                        account.balance = Decimal(str(account_data.balance))
                    
                    if hasattr(account_data, 'available_balance'):
                        account.available_balance = Decimal(str(account_data.available_balance))
                
                # 更新其他字段
                account.updated_at = datetime.now()
                
                logger.debug("账户数据已更新",
                           account_index=account_index,
                           balance=float(account.balance),
                           available_balance=float(account.available_balance))
            else:
                logger.warning("收到未知账户的更新",
                             account_index=account_index)
                             
        except Exception as e:
            logger.error("更新账户数据失败",
                        account_index=account_index,
                        error=str(e))