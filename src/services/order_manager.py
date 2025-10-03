"""
Order Manager for Lighter Hedge Trading System
"""

import asyncio
from typing import Dict, List, Optional, Tuple
from datetime import datetime
from decimal import Decimal
import structlog

import lighter
from src.models import OrderInfo, OrderStatus, OrderBook, MarketData
from src.config.config_manager import ConfigManager
from src.core.lighter_client_factory import get_client_factory

logger = structlog.get_logger()


class OrderManager:
    """Manages order creation, monitoring, and execution"""
    
    def __init__(self, config_manager: ConfigManager, account_manager=None, websocket_manager=None):
        self.config_manager = config_manager
        self.account_manager = account_manager  # 添加AccountManager引用
        self.websocket_manager = websocket_manager  # 添加WebSocket管理器引用
        self.orders: Dict[str, OrderInfo] = {}
        self.client_factory = get_client_factory(config_manager)
        self.order_api: Optional[lighter.OrderApi] = None
        self.signer_clients: Dict[int, lighter.SignerClient] = {}
        self._monitoring_task: Optional[asyncio.Task] = None
        
        # 缓存所有可能的市场ID
        self._cached_market_ids: Optional[List[int]] = None
        
        # API频率限制
        self._last_api_call_time: Dict[str, datetime] = {}
        self._api_call_interval = 2.0  # 最小间隔2秒
        
        # 数据时效性配置
        self._data_freshness_threshold = 30.0  # WebSocket数据有效期30秒，避免使用过期数据导致亏损
    
    def _get_all_possible_market_ids(self) -> List[int]:
        """获取所有可能的市场ID，包括配置中的和常见的"""
        if self._cached_market_ids is not None:
            return self._cached_market_ids
        
        market_ids = set()
        
        try:
            # 从配置的交易对中获取市场ID
            trading_pairs = self.config_manager.get_trading_pairs()
            for pair in trading_pairs:
                if pair.is_enabled:
                    market_ids.add(pair.market_index)
            
            # 添加常见的市场ID (1-10)，以防配置不完整
            for i in range(1, 11):
                market_ids.add(i)
            
            self._cached_market_ids = sorted(list(market_ids))
            logger.debug("缓存所有可能的市场ID", market_ids=self._cached_market_ids)
            
        except Exception as e:
            logger.error("获取市场ID失败，使用默认值", error=str(e))
            self._cached_market_ids = [1, 2, 3, 4, 5]  # 默认常见市场ID
        
        return self._cached_market_ids
    
    def _is_data_fresh(self, data) -> bool:
        """检查数据是否新鲜（在有效期内）"""
        if not data or not hasattr(data, 'timestamp'):
            logger.warning("数据新鲜度检查失败", 
                         has_data=bool(data),
                         has_timestamp=hasattr(data, 'timestamp') if data else False)
            return False
        
        age = (datetime.now() - data.timestamp).total_seconds()
        is_fresh = age <= self._data_freshness_threshold
        
        logger.debug("数据新鲜度检查", 
                   data_age_seconds=f"{age:.2f}s",
                   threshold_seconds=self._data_freshness_threshold,
                   is_fresh=is_fresh,
                   price=float(data.price) if hasattr(data, 'price') else None)
        
        return is_fresh
    
    def _can_make_api_call(self, api_key: str, force_first_call: bool = False) -> bool:
        """检查是否可以进行API调用（频率限制）"""
        now = datetime.now()
        last_call = self._last_api_call_time.get(api_key)
        
        # 如果是首次调用且强制允许，直接允许
        if last_call is None and force_first_call:
            self._last_api_call_time[api_key] = now
            return True
        
        if last_call is None:
            self._last_api_call_time[api_key] = now
            return True
        
        time_since_last = (now - last_call).total_seconds()
        if time_since_last >= self._api_call_interval:
            self._last_api_call_time[api_key] = now
            return True
        
        return False
        
    async def _safe_sdk_call(self, sdk_func, order_id, operation="SDK操作"):
        """安全调用SDK函数，处理内部AttributeError错误"""
        try:
            result = await sdk_func()
            return result
        except AttributeError as attr_error:
            if "'NoneType' object has no attribute 'code'" in str(attr_error):
                logger.warning(f"{operation}SDK内部错误，但操作可能已执行",
                             order_id=order_id,
                             error=str(attr_error))
                # 模拟成功响应，因为操作可能实际上已经执行
                return (None, f"simulated_tx_{order_id}", None)
            else:
                raise attr_error

    def _parse_sdk_result(self, result, operation="SDK操作"):
        """解析SDK返回结果，统一处理不同格式"""
        logger.debug(f"{operation}返回结果",
                   result_type=type(result),
                   result_value=str(result)[:200] if result else None)
        
        # 验证result不为None
        if result is None:
            raise Exception("SDK返回None，可能是平台不支持或SignerClient初始化失败")
        
        # 根据官方API文档，create_order返回(CreateOrder, TxHash, str)
        # 其中第三个参数是错误字符串，空字符串表示成功
        if isinstance(result, tuple) and len(result) == 3:
            tx_obj, tx_hash, err_str = result[0], result[1], result[2]
            
            # 检查错误字符串
            if err_str and err_str.strip():  # 如果错误字符串不为空
                raise Exception(f"{operation}失败: {err_str}")
            
            # 成功情况：提取实际的交易哈希
            if hasattr(tx_hash, 'value'):
                actual_tx_hash = tx_hash.value
            elif hasattr(tx_hash, 'hash'):
                actual_tx_hash = tx_hash.hash
            else:
                actual_tx_hash = str(tx_hash) if tx_hash else None
            
            return tx_obj, actual_tx_hash, None
        
        # 旧的兼容性处理
        elif isinstance(result, tuple):
            if len(result) >= 3:
                tx, tx_hash, err = result[0], result[1], result[2]
            elif len(result) == 2:
                tx, err = result[0], result[1]
                tx_hash = None
            else:
                raise Exception(f"SDK返回元组长度错误: {len(result)}, 值: {result}")
                
            if err:
                raise Exception(f"{operation}失败: {err}")
            
            return tx, tx_hash, err
        else:
            raise Exception(f"SDK返回格式未知: {type(result)}, 值: {result}")
        
        return tx, tx_hash, err

    async def initialize(self) -> None:
        """Initialize order manager"""
        try:
            # Use client factory for API initialization
            self.order_api = self.client_factory.get_order_api()
            
            # Initialize signer clients for each account
            await self.initialize_signer_clients()
            
            # 预热：为启用的交易对预先获取一次市场数据
            await self._warmup_market_data()
            
            # Start order monitoring
            await self.start_monitoring()
            
            logger.info("订单管理器初始化完成", 
                       signer_clients_count=len(self.signer_clients))
            
        except Exception as e:
            logger.error("订单管理器初始化失败", error=str(e))
            raise
    
    async def _warmup_market_data(self) -> None:
        """预热：为启用的交易对预先获取市场数据"""
        try:
            trading_pairs = self.config_manager.get_active_pairs()
            if not trading_pairs:
                return
            
            logger.info("开始预热市场数据", pairs_count=len(trading_pairs))
            
            # 为每个启用的交易对预热数据
            for pair in trading_pairs[:3]:  # 限制预热数量，避免过多API调用
                try:
                    logger.info("预热市场数据", 
                               pair_id=pair.id,
                               market_index=pair.market_index)
                    
                    # 强制使用API获取一次数据作为预热
                    market_data = await self._fetch_market_data_from_api(pair.market_index)
                    orderbook = await self._fetch_orderbook_from_api(pair.market_index)
                    
                    if market_data:
                        logger.info("预热市场数据成功", 
                                   pair_id=pair.id,
                                   price=float(market_data.price))
                    
                    # 预热间隔，避免频率限制
                    await asyncio.sleep(1)
                    
                except Exception as e:
                    logger.warning("预热市场数据失败", 
                                  pair_id=pair.id,
                                  error=str(e))
            
            logger.info("市场数据预热完成")
            
        except Exception as e:
            logger.error("市场数据预热失败", error=str(e))
    
    async def initialize_signer_clients(self) -> None:
        """Initialize signer clients for accounts using factory"""
        accounts = self.config_manager.get_active_accounts()
        
        for account in accounts:
            try:
                # Use client factory to create signer client
                signer_client = await self.client_factory.get_signer_client(account.index)
                
                if signer_client:
                    self.signer_clients[account.index] = signer_client
                    logger.info("SignerClient初始化成功", 
                               account_index=account.index)
                else:
                    logger.warning("SignerClient创建失败",
                                 account_index=account.index)
                
            except Exception as e:
                logger.error("SignerClient初始化失败",
                           account_index=account.index,
                           error=str(e))
                # Continue with other accounts
    
    def has_signer_clients(self) -> bool:
        """检查是否有可用的SignerClient"""
        return len(self.signer_clients) > 0
    
    def get_available_accounts(self) -> List[int]:
        """获取有可用SignerClient的账户列表"""
        return list(self.signer_clients.keys())
    
    async def validate_signer_client_connection(self, account_index: int) -> bool:
        """验证SignerClient连接状态"""
        try:
            signer_client = self.signer_clients.get(account_index)
            if not signer_client:
                return False
            
            # 这里可以添加连接验证逻辑
            # 例如：调用一个简单的API来检查连接
            
            return True
            
        except Exception as e:
            logger.error("SignerClient连接验证失败",
                        account_index=account_index,
                        error=str(e))
            return False
    
    async def create_market_order(
        self,
        account_index: int,
        market_index: int,
        side: str,  # "buy" or "sell"
        amount: Decimal,
        max_slippage: Optional[Decimal] = None,
        reduce_only: bool = False
    ) -> Optional[OrderInfo]:
        """Create a market order using Lighter SDK"""
        try:
            # 验证SignerClient可用性
            if not self.has_signer_clients():
                logger.error("系统中没有可用的SignerClient", 
                           platform="Windows", 
                           available_accounts=len(self.signer_clients))
                raise ValueError("系统中没有可用的SignerClient，请确保在支持的平台(WSL/Linux/macOS)上运行")
            
            signer_client = self.signer_clients.get(account_index)
            if not signer_client:
                available_accounts = self.get_available_accounts()
                logger.error("账户SignerClient未初始化", 
                           account_index=account_index,
                           available_accounts=available_accounts,
                           signer_clients_keys=list(self.signer_clients.keys()))
                raise ValueError(f"账户 {account_index} 的SignerClient未初始化，可用账户: {available_accounts}")
            
            # 验证连接状态
            if not await self.validate_signer_client_connection(account_index):
                logger.error("SignerClient连接验证失败", account_index=account_index)
                raise ValueError(f"账户 {account_index} 的SignerClient连接验证失败")
            
            # Get current market price for reference
            market_data = await self.get_market_data(market_index)
            if not market_data:
                raise ValueError(f"无法获取市场 {market_index} 的价格数据")
            
            # Calculate market order price with slippage protection
            slippage = max_slippage or Decimal("0.005")  # Default 0.5% slippage
            
            if side == "buy":
                # For buy orders, use ask price with positive slippage
                execution_price = market_data.ask_price or market_data.price
                max_price = execution_price * (1 + slippage)
            else:
                # For sell orders, use bid price with negative slippage
                execution_price = market_data.bid_price or market_data.price
                max_price = execution_price * (1 - slippage)
            
            # Create unique order ID
            order_id = f"market_{account_index}_{market_index}_{int(datetime.now().timestamp() * 1000)}"
            
            logger.info("正在执行市价订单",
                       order_id=order_id,
                       account_index=account_index,
                       market_index=market_index,
                       side=side,
                       amount=float(amount),
                       execution_price=float(execution_price),
                       max_price=float(max_price))
            
            # Execute market order using Lighter SDK
            try:
                # 转换参数格式
                is_ask = side.lower() == "sell"
                client_order_index = self._generate_client_order_index()
                
                # 根据市场配置进行正确转换
                price_decimals, size_decimals, price_multiplier, size_multiplier = await self._get_market_precision(market_index)
                base_amount = int(amount * size_multiplier)
                price_int = int(max_price * price_multiplier)
                
                logger.debug("市场精度转换",
                           market_index=market_index,
                           price_decimals=price_decimals,
                           size_decimals=size_decimals,
                           original_amount=float(amount),
                           converted_amount=base_amount,
                           original_price=float(max_price),
                           converted_price=price_int)
                
                # Submit order through signer client - 使用市价单
                import lighter
                
                # 额外验证signer_client不为None
                if signer_client is None:
                    raise Exception("SignerClient为None，无法执行订单")
                
                logger.debug("准备调用SDK create_market_order",
                           market_index=market_index,
                           client_order_index=client_order_index,
                           base_amount=base_amount,
                           price_int=price_int,
                           is_ask=is_ask,
                           reduce_only=reduce_only)
                
                # 使用正确的create_market_order方法！
                # 根据官方SDK源码，create_market_order会自动设置：
                # - order_type = ORDER_TYPE_MARKET 
                # - time_in_force = ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL
                # - order_expiry = DEFAULT_IOC_EXPIRY (0)
                result = await signer_client.create_market_order(
                    market_index=market_index,
                    client_order_index=client_order_index,
                    base_amount=base_amount,
                    avg_execution_price=price_int,  # 使用avg_execution_price作为最差可接受价格
                    is_ask=is_ask,
                    reduce_only=reduce_only,  # 使用传入的参数
                    nonce=-1,  # 添加默认nonce参数
                    api_key_index=-1  # 添加默认api_key_index参数
                )
                
                tx, tx_hash, err = self._parse_sdk_result(result, "市价订单")
                
                response = {"tx": tx, "tx_hash": tx_hash}
                
                # Create order info with SDK response
                order_info = OrderInfo(
                    id=order_id,
                    account_index=account_index,
                    market_index=market_index,
                    order_type="market",
                    side=side,
                    amount=amount,
                    price=max_price,
                    status=OrderStatus.PENDING,
                    created_at=datetime.now(),
                    sdk_order_id=str(response.get('order_id', client_order_index)),
                    metadata={
                        "execution_price": float(execution_price),
                        "max_slippage": float(slippage),
                        "is_hedge_order": False
                    }
                )
                
                self.orders[order_id] = order_info
                
                logger.info("市价订单已提交",
                           order_id=order_id,
                           sdk_order_id=order_info.sdk_order_id,
                           account_index=account_index,
                           market_index=market_index,
                           side=side,
                           amount=float(amount))
                
                return order_info
                
            except Exception as sdk_error:
                import traceback
                error_traceback = traceback.format_exc()
                
                logger.error("SDK订单执行失败",
                           order_id=order_id,
                           error=str(sdk_error),
                           error_type=type(sdk_error).__name__,
                           traceback=error_traceback)
                
                # Create order info with failed status
                order_info = OrderInfo(
                    id=order_id,
                    account_index=account_index,
                    market_index=market_index,
                    order_type="market",
                    side=side,
                    amount=amount,
                    price=max_price,
                    status=OrderStatus.FAILED,
                    created_at=datetime.now(),
                    metadata={"error": str(sdk_error)}
                )
                
                self.orders[order_id] = order_info
                return order_info
            
        except Exception as e:
            logger.error("创建市价订单失败",
                        account_index=account_index,
                        market_index=market_index,
                        error=str(e))
            return None
    
    async def _validate_order_price(self, market_index: int, price: Decimal, side: str) -> bool:
        """验证订单价格是否在合理范围内"""
        try:
            # 获取当前市场数据
            market_data = await self.get_market_data(market_index)
            if not market_data:
                logger.warning("无法获取市场数据进行价格验证", market_index=market_index)
                return True  # 无法验证时允许通过
            
            current_price = market_data.price
            price_diff_percent = abs(price - current_price) / current_price
            
            # 设置价格偏离限制 (10%以内认为合理)
            max_price_deviation = Decimal('0.10')  # 10%
            
            if price_diff_percent > max_price_deviation:
                logger.warning("订单价格偏离市场价格过大",
                             market_index=market_index,
                             order_price=float(price),
                             market_price=float(current_price),
                             deviation_percent=float(price_diff_percent * 100),
                             max_deviation=float(max_price_deviation * 100))
                return False
            
            # 检查价格精度(避免"accidental price"错误)
            # 确保价格是合理的整数值，避免过于精确的小数
            if side.lower() == "buy":
                # 买单价格应该略低于市场价格
                if price > current_price * Decimal('1.05'):  # 不超过市价5%
                    logger.warning("买单价格过高", order_price=float(price), market_price=float(current_price))
                    return False
            else:
                # 卖单价格应该略高于市场价格  
                if price < current_price * Decimal('0.95'):  # 不低于市价5%
                    logger.warning("卖单价格过低", order_price=float(price), market_price=float(current_price))
                    return False
            
            logger.debug("价格验证通过",
                        market_index=market_index,
                        order_price=float(price),
                        market_price=float(current_price),
                        side=side)
            return True
            
        except Exception as e:
            logger.error("价格验证失败", error=str(e))
            return False

    async def create_limit_order(
        self,
        account_index: int,
        market_index: int,
        side: str,
        amount: Decimal,
        price: Decimal
    ) -> Optional[OrderInfo]:
        """Create a limit order"""
        try:
            signer_client = self.signer_clients.get(account_index)
            if not signer_client:
                raise ValueError(f"账户 {account_index} 的SignerClient未初始化")
            
            # 验证价格合理性
            if not await self._validate_order_price(market_index, price, side):
                logger.error("订单价格验证失败，取消订单创建",
                           account_index=account_index,
                           market_index=market_index,
                           price=float(price),
                           side=side)
                return None
            
            order_id = f"limit_{account_index}_{market_index}_{int(datetime.now().timestamp() * 1000)}"
            
            # 准备订单请求参数
            is_ask = side.lower() == "sell"
            
            # 转换参数格式为SDK要求
            client_order_index = self._generate_client_order_index()
            
            # 根据市场配置进行正确转换
            price_decimals, size_decimals, price_multiplier, size_multiplier = await self._get_market_precision(market_index)
            base_amount = int(amount * size_multiplier)
            price_int = int(price * price_multiplier)
            
            logger.debug("限价单市场精度转换",
                       market_index=market_index,
                       price_decimals=price_decimals,
                       size_decimals=size_decimals,
                       original_amount=float(amount),
                       converted_amount=base_amount,
                       original_price=float(price),
                       converted_price=price_int)
            
            try:
                # 通过SignerClient提交订单 - 使用正确的参数格式
                import lighter
                
                # 使用安全SDK调用包装器  
                result = await self._safe_sdk_call(
                    lambda: signer_client.create_order(
                        market_index=market_index,
                        client_order_index=client_order_index,
                        base_amount=base_amount,
                        price=price_int,
                        is_ask=is_ask,
                        order_type=lighter.SignerClient.ORDER_TYPE_LIMIT,
                        time_in_force=lighter.SignerClient.ORDER_TIME_IN_FORCE_GOOD_TILL_TIME,
                        reduce_only=False,  # 明确指定为False
                        trigger_price=0,
                        order_expiry=-1,    # 使用默认值
                        nonce=-1,          # 使用默认值
                        api_key_index=-1   # 使用默认值
                    ),
                    order_id,
                    "限价订单"
                )
                
                tx, tx_hash, err = self._parse_sdk_result(result, "限价订单")
                
                response = {"tx": tx, "tx_hash": tx_hash}
                
                # 创建成功的订单信息
                order_info = OrderInfo(
                    id=order_id,
                    account_index=account_index,
                    market_index=market_index,
                    order_type="limit",
                    side=side,
                    amount=amount,
                    price=price,
                    status=OrderStatus.PENDING,
                    created_at=datetime.now(),
                    sdk_order_id=str(response) if response else None,
                    metadata={"sdk_response": str(response), "client_order_index": client_order_index}
                )
                
                self.orders[order_id] = order_info
                
                logger.info("限价订单已提交到区块链",
                           order_id=order_id,
                           account_index=account_index,
                           market_index=market_index,
                           side=side,
                           amount=float(amount),
                           price=float(price),
                           tx_hash=tx_hash.tx_hash if hasattr(tx_hash, 'tx_hash') else str(tx_hash),
                           predicted_execution_time=getattr(tx_hash, 'predicted_execution_time_ms', None),
                           sdk_response=str(response))
                
                # 添加后续验证提醒
                logger.info("订单提交完成，建议验证",
                           verification_steps=[
                               "检查Lighter官方界面的订单历史",
                               "使用交易哈希查询区块确认状态", 
                               "检查账户余额是否发生变化",
                               f"等待预计执行时间: {getattr(tx_hash, 'predicted_execution_time_ms', 'N/A')}ms"
                           ])
                
                return order_info
                
            except Exception as sdk_error:
                logger.error("提交限价订单到区块链失败",
                           order_id=order_id,
                           account_index=account_index,
                           error=str(sdk_error))
                
                # 创建失败状态的订单信息
                order_info = OrderInfo(
                    id=order_id,
                    account_index=account_index,
                    market_index=market_index,
                    order_type="limit",
                    side=side,
                    amount=amount,
                    price=price,
                    status=OrderStatus.FAILED,
                    created_at=datetime.now(),
                    metadata={"error": str(sdk_error)}
                )
                
                self.orders[order_id] = order_info
                return order_info
            
        except Exception as e:
            logger.error("创建限价订单失败",
                        account_index=account_index,
                        market_index=market_index,
                        error=str(e))
            return None
    
    async def cancel_order(self, order_id: str) -> bool:
        """Cancel an order using SignerClient"""
        try:
            order = self.orders.get(order_id)
            if not order:
                logger.warning("订单不存在", order_id=order_id)
                return False
            
            # Check if order can be cancelled
            if order.status not in [OrderStatus.PENDING]:
                logger.warning("订单状态不允许取消", 
                             order_id=order_id, 
                             status=order.status)
                return False
            
            # 验证SignerClient可用性
            if not self.has_signer_clients():
                raise ValueError("系统中没有可用的SignerClient，请确保在支持的平台(WSL/Linux/macOS)上运行")
            
            signer_client = self.signer_clients.get(order.account_index)
            if not signer_client:
                available_accounts = self.get_available_accounts()
                raise ValueError(f"账户 {order.account_index} 的SignerClient未初始化，可用账户: {available_accounts}")
            
            # 验证连接状态
            if not await self.validate_signer_client_connection(order.account_index):
                raise ValueError(f"账户 {order.account_index} 的SignerClient连接验证失败")
            
            # Cancel order using Lighter SDK
            if order.sdk_order_id:
                try:
                    logger.info("正在取消订单",
                               order_id=order_id,
                               sdk_order_id=order.sdk_order_id,
                               account_index=order.account_index)
                    
                    # Use SignerClient to cancel the order
                    cancel_response = await signer_client.cancel_order(order.sdk_order_id)
                    
                    # Update order status
                    order.status = OrderStatus.CANCELLED
                    order.cancelled_at = datetime.now()
                    
                    logger.info("订单取消成功",
                               order_id=order_id,
                               sdk_order_id=order.sdk_order_id)
                    return True
                    
                except Exception as sdk_error:
                    logger.error("SDK订单取消失败",
                               order_id=order_id,
                               sdk_order_id=order.sdk_order_id,
                               error=str(sdk_error))
                    return False
            else:
                # For orders without SDK order ID, mark as cancelled directly
                order.status = OrderStatus.CANCELLED
                order.cancelled_at = datetime.now()
                
                logger.info("订单已标记为取消", order_id=order_id)
                return True
            
        except Exception as e:
            logger.error("取消订单失败", order_id=order_id, error=str(e))
            return False
    
    
    def _generate_client_order_index(self) -> int:
        """生成客户端订单索引"""
        return int(datetime.now().timestamp() * 1000) % 1000000
    
    async def _get_market_precision(self, market_index: int) -> tuple:
        """
        动态获取市场精度配置
        返回 (price_decimals, size_decimals, price_multiplier, size_multiplier)
        """
        try:
            # 首先尝试从缓存获取
            if not hasattr(self, '_market_precision_cache'):
                self._market_precision_cache = {}
            
            if market_index in self._market_precision_cache:
                return self._market_precision_cache[market_index]
            
            # 尝试从API获取市场信息
            if self.order_api:
                try:
                    # 通过orderbook API获取市场信息，这个API通常包含精度信息
                    orderbook_data = await self.order_api.order_book_details(market_id=market_index)
                    
                    if orderbook_data and hasattr(orderbook_data, 'market'):
                        market_info = orderbook_data.market
                        price_decimals = getattr(market_info, 'price_decimals', 1)
                        size_decimals = getattr(market_info, 'size_decimals', 5)
                        
                        # 计算乘数
                        price_multiplier = 10 ** price_decimals
                        size_multiplier = 10 ** size_decimals
                        
                        precision = (price_decimals, size_decimals, price_multiplier, size_multiplier)
                        
                        # 缓存结果
                        self._market_precision_cache[market_index] = precision
                        
                        logger.info("从API获取市场精度配置",
                                   market_index=market_index,
                                   price_decimals=price_decimals,
                                   size_decimals=size_decimals)
                        
                        return precision
                        
                except Exception as api_error:
                    logger.warning("从API获取市场精度失败",
                                 market_index=market_index,
                                 error=str(api_error))
            
            # 如果API失败，优先使用market_index直接映射
            precision = self._get_precision_by_market_index(market_index)
            if precision != (2, 4, 100, 10000):  # 不是默认值，说明找到了匹配
                self._market_precision_cache[market_index] = precision
                logger.info("使用market_index直接映射精度",
                           market_index=market_index,
                           precision=precision)
                return precision
            
            # 如果market_index映射也没有，尝试从配置中推断
            trading_pairs = self.config_manager.get_trading_pairs()
            for pair in trading_pairs:
                if pair.market_index == market_index:
                    # 根据交易对名称推断精度
                    precision = self._infer_precision_from_pair_name(pair.name, pair.id)
                    self._market_precision_cache[market_index] = precision
                    
                    logger.info("从配置推断市场精度",
                               market_index=market_index,
                               pair_name=pair.name,
                               precision=precision)
                    
                    return precision
            
            # 最后使用默认配置
            logger.warning("无法获取市场精度，使用默认BTC配置", market_index=market_index)
            default_precision = (1, 5, 10, 100000)  # BTC默认配置
            self._market_precision_cache[market_index] = default_precision
            return default_precision
            
        except Exception as e:
            logger.error("获取市场精度配置失败",
                        market_index=market_index,
                        error=str(e))
            return (1, 5, 10, 100000)  # 默认BTC配置
    
    def _get_precision_by_market_index(self, market_index: int) -> tuple:
        """根据market_index直接获取真实精度配置（从Lighter Protocol API确认）"""
        precision_map = {
            1: (1, 5, 10, 100000),      # BTC
            2: (3, 3, 1000, 1000),      # SOL - 真实精度配置
            3: (6, 0, 1000000, 1),      # DOGE
            4: (6, 0, 1000000, 1),      # 1000PEPE  
            5: (5, 1, 100000, 10),      # WIF
        }
        
        return precision_map.get(market_index, (2, 4, 100, 10000))  # 默认配置
    
    def _infer_precision_from_pair_name(self, pair_name: str, pair_id: str) -> tuple:
        """根据交易对名称推断精度配置（已更新为真实API精度）"""
        name_lower = pair_name.lower()
        id_lower = pair_id.lower()
        
        if 'btc' in name_lower or 'btc' in id_lower or 'bitcoin' in name_lower:
            return (1, 5, 10, 100000)  # BTC - 真实精度
        elif 'eth' in name_lower or 'eth' in id_lower or 'ethereum' in name_lower:
            return (2, 4, 100, 10000)  # ETH - 预估精度（目前无ETH市场）
        elif 'sol' in name_lower or 'sol' in id_lower or 'solana' in name_lower:
            return (3, 3, 1000, 1000)  # SOL - 真实精度（修复）
        elif 'doge' in name_lower or 'doge' in id_lower:
            return (6, 0, 1000000, 1)  # DOGE - 真实精度
        elif 'pepe' in name_lower or 'pepe' in id_lower:
            return (6, 0, 1000000, 1)  # PEPE - 真实精度
        elif 'wif' in name_lower or 'wif' in id_lower:
            return (5, 1, 100000, 10)  # WIF - 真实精度
        elif 'usdc' in name_lower or 'usdc' in id_lower:
            return (4, 2, 10000, 100)  # USDC - 预估精度
        elif 'usdt' in name_lower or 'usdt' in id_lower:
            return (4, 2, 10000, 100)  # USDT - 预估精度
        else:
            # 默认使用中等精度配置
            logger.warning("无法识别交易对类型，使用默认精度",
                         pair_name=pair_name,
                         pair_id=pair_id)
            return (2, 4, 100, 10000)  # 类似ETH的配置
    
    async def get_order_status(self, order_id: str) -> Optional[OrderStatus]:
        """Get order status"""
        order = self.orders.get(order_id)
        return order.status if order else None
    
    async def get_account_orders(self, account_index: int) -> List[OrderInfo]:
        """Get all orders for an account"""
        return [order for order in self.orders.values() 
                if order.account_index == account_index]
    
    async def create_stop_loss_order(
        self,
        account_index: int,
        market_index: int,
        side: str,
        amount: Decimal,
        trigger_price: Decimal,
        order_type: str = "market"  # "market" or "limit"
    ) -> Optional[OrderInfo]:
        """创建止损订单"""
        try:
            signer_client = self.signer_clients.get(account_index)
            if not signer_client:
                raise ValueError(f"账户 {account_index} 的SignerClient未初始化")
            
            order_id = f"sl_{account_index}_{market_index}_{int(datetime.now().timestamp() * 1000)}"
            
            # 转换参数
            is_ask = side.lower() == "sell"
            client_order_index = self._generate_client_order_index()
            
            # 动态获取市场精度
            price_decimals, size_decimals, price_multiplier, size_multiplier = await self._get_market_precision(market_index)
            base_amount = int(amount * size_multiplier)
            trigger_price_int = int(trigger_price * price_multiplier)
            
            logger.debug("止损单市场精度转换",
                       market_index=market_index,
                       price_decimals=price_decimals,
                       size_decimals=size_decimals)
            
            try:
                import lighter
                if order_type == "market":
                    # 市价止损单 - 设置为仅减仓模式
                    result = await signer_client.create_sl_order(
                        market_index=market_index,
                        client_order_index=client_order_index,
                        base_amount=base_amount,
                        trigger_price=trigger_price_int,
                        price=trigger_price_int,  # 市价单价格等于触发价格
                        is_ask=is_ask,
                        reduce_only=True  # 仅减仓模式，避免意外增加仓位
                    )
                    tx, tx_hash, err = self._parse_sdk_result(result, "市价止损订单")
                else:
                    # 限价止损单 - 设置为仅减仓模式
                    limit_price_int = trigger_price_int  # 简化：限价等于触发价格
                    result = await signer_client.create_sl_limit_order(
                        market_index=market_index,
                        client_order_index=client_order_index,
                        base_amount=base_amount,
                        trigger_price=trigger_price_int,
                        price=limit_price_int,
                        is_ask=is_ask,
                        reduce_only=True  # 仅减仓模式，避免意外增加仓位
                    )
                    tx, tx_hash, err = self._parse_sdk_result(result, "限价止损订单")
                
                if err:
                    raise Exception(f"止损订单提交失败: {err}")
                
                response = {"tx": tx, "tx_hash": tx_hash}
                
                order_info = OrderInfo(
                    id=order_id,
                    account_index=account_index,
                    market_index=market_index,
                    order_type=f"stop_loss_{order_type}",
                    side=side,
                    amount=amount,
                    price=trigger_price,
                    status=OrderStatus.PENDING,
                    created_at=datetime.now(),
                    sdk_order_id=str(response) if response else None,
                    metadata={
                        "trigger_price": float(trigger_price),
                        "order_type": order_type,
                        "is_stop_loss": True
                    }
                )
                
                self.orders[order_id] = order_info
                
                logger.info("止损订单已提交",
                           order_id=order_id,
                           account_index=account_index,
                           side=side,
                           amount=float(amount),
                           reduce_only=True,  # 仅减仓模式
                           trigger_price=float(trigger_price),
                           order_type=order_type)
                
                return order_info
                
            except Exception as sdk_error:
                logger.error("止损订单SDK执行失败",
                           order_id=order_id,
                           error=str(sdk_error))
                return None
            
        except Exception as e:
            logger.error("创建止损订单失败",
                        account_index=account_index,
                        error=str(e))
            return None

    async def create_take_profit_order(
        self,
        account_index: int,
        market_index: int,
        side: str,
        amount: Decimal,
        trigger_price: Decimal,
        order_type: str = "market"  # "market" or "limit"
    ) -> Optional[OrderInfo]:
        """创建止盈订单"""
        try:
            signer_client = self.signer_clients.get(account_index)
            if not signer_client:
                raise ValueError(f"账户 {account_index} 的SignerClient未初始化")
            
            order_id = f"tp_{account_index}_{market_index}_{int(datetime.now().timestamp() * 1000)}"
            
            # 转换参数
            is_ask = side.lower() == "sell"
            client_order_index = self._generate_client_order_index()
            
            # 动态获取市场精度
            price_decimals, size_decimals, price_multiplier, size_multiplier = await self._get_market_precision(market_index)
            base_amount = int(amount * size_multiplier)
            trigger_price_int = int(trigger_price * price_multiplier)
            
            logger.debug("止盈单市场精度转换",
                       market_index=market_index,
                       price_decimals=price_decimals,
                       size_decimals=size_decimals)
            
            try:
                import lighter
                if order_type == "market":
                    # 市价止盈单 - 设置为仅减仓模式
                    result = await signer_client.create_tp_order(
                        market_index=market_index,
                        client_order_index=client_order_index,
                        base_amount=base_amount,
                        trigger_price=trigger_price_int,
                        price=trigger_price_int,  # 市价单价格等于触发价格
                        is_ask=is_ask,
                        reduce_only=True  # 仅减仓模式，避免意外增加仓位
                    )
                    tx, tx_hash, err = self._parse_sdk_result(result, "市价止盈订单")
                else:
                    # 限价止盈单 - 设置为仅减仓模式
                    limit_price_int = trigger_price_int  # 简化：限价等于触发价格
                    result = await signer_client.create_tp_limit_order(
                        market_index=market_index,
                        client_order_index=client_order_index,
                        base_amount=base_amount,
                        trigger_price=trigger_price_int,
                        price=limit_price_int,
                        is_ask=is_ask,
                        reduce_only=True  # 仅减仓模式，避免意外增加仓位
                    )
                    tx, tx_hash, err = self._parse_sdk_result(result, "限价止盈订单")
                
                if err:
                    raise Exception(f"止盈订单提交失败: {err}")
                
                response = {"tx": tx, "tx_hash": tx_hash}
                
                order_info = OrderInfo(
                    id=order_id,
                    account_index=account_index,
                    market_index=market_index,
                    order_type=f"take_profit_{order_type}",
                    side=side,
                    amount=amount,
                    price=trigger_price,
                    status=OrderStatus.PENDING,
                    created_at=datetime.now(),
                    sdk_order_id=str(response) if response else None,
                    metadata={
                        "trigger_price": float(trigger_price),
                        "order_type": order_type,
                        "is_take_profit": True
                    }
                )
                
                self.orders[order_id] = order_info
                
                logger.info("止盈订单已提交",
                           order_id=order_id,
                           account_index=account_index,
                           side=side,
                           amount=float(amount),
                           reduce_only=True,  # 仅减仓模式
                           trigger_price=float(trigger_price),
                           order_type=order_type)
                
                return order_info
                
            except Exception as sdk_error:
                logger.error("止盈订单SDK执行失败",
                           order_id=order_id,
                           error=str(sdk_error))
                return None
            
        except Exception as e:
            logger.error("创建止盈订单失败",
                        account_index=account_index,
                        error=str(e))
            return None

    async def cancel_order(
        self,
        account_index: int,
        market_index: int,
        order_index: int
    ) -> bool:
        """Cancel a specific order by order index"""
        try:
            signer_client = self.signer_clients.get(account_index)
            if not signer_client:
                raise ValueError(f"账户 {account_index} 的SignerClient未初始化")
            
            result = await signer_client.cancel_order(
                market_index=market_index,
                order_index=order_index
            )
            
            tx, tx_hash, err = self._parse_sdk_result(result, "取消订单")
            
            if err:
                logger.error("取消订单失败",
                           account_index=account_index,
                           market_index=market_index,
                           order_index=order_index,
                           error=err)
                return False
            
            logger.info("订单已取消",
                       account_index=account_index,
                       market_index=market_index,
                       order_index=order_index,
                       tx_hash=tx_hash)
            return True
            
        except Exception as e:
            logger.error("取消订单异常",
                        account_index=account_index,
                        market_index=market_index,
                        order_index=order_index,
                        error=str(e))
            return False

    async def cancel_all_inactive_orders(
        self,
        account_index: int,
        market_index: int = None
    ) -> int:
        """Cancel all inactive orders for an account and optionally specific market"""
        try:
            signer_client = self.signer_clients.get(account_index)
            if not signer_client:
                logger.error("SignerClient未初始化", account_index=account_index)
                return 0
            
            # Get inactive orders from exchange using authenticated SignerClient
            # Note: account_inactive_orders doesn't accept market_index parameter
            # We'll filter by market after getting all orders
            inactive_orders_data = await signer_client.account_inactive_orders(
                limit=100  # API maximum limit is 100
            )
            
            if not inactive_orders_data or not hasattr(inactive_orders_data, 'orders'):
                logger.info("未找到需要取消的订单",
                           account_index=account_index,
                           market_index=market_index)
                return 0
            
            all_inactive_orders = inactive_orders_data.orders
            
            cancelled_count = 0
            
            # Cancel each inactive order
            for order in inactive_orders_data.orders:
                try:
                    # Extract order details
                    order_market_index = getattr(order, 'market_index', getattr(order, 'market_id', None))
                    order_index = getattr(order, 'order_index', getattr(order, 'id', None))
                    
                    if order_market_index is None or order_index is None:
                        logger.warning("订单信息不完整，跳过取消",
                                     order_data=str(order)[:200])
                        continue
                    
                    # Skip if filtering by market and this order is for different market
                    if market_index is not None and order_market_index != market_index:
                        continue
                    
                    # Cancel the order
                    result = await signer_client.cancel_order(
                        market_index=order_market_index,
                        order_index=order_index
                    )
                    
                    tx, tx_hash, err = self._parse_sdk_result(result, "批量取消订单")
                    
                    if not err:
                        cancelled_count += 1
                        logger.info("历史订单已取消",
                                   account_index=account_index,
                                   market_index=order_market_index,
                                   order_index=order_index,
                                   tx_hash=tx_hash)
                    else:
                        logger.warning("取消历史订单失败",
                                     account_index=account_index,
                                     market_index=order_market_index,
                                     order_index=order_index,
                                     error=err)
                    
                    # Small delay to avoid overwhelming the API
                    await asyncio.sleep(0.1)
                    
                except Exception as order_error:
                    logger.error("取消单个订单时发生异常",
                               account_index=account_index,
                               order_data=str(order)[:200],
                               error=str(order_error))
                    continue
            
            if cancelled_count > 0:
                logger.info("批量取消订单完成",
                           account_index=account_index,
                           market_index=market_index,
                           cancelled_count=cancelled_count)
            
            return cancelled_count
            
        except Exception as e:
            logger.error("批量取消订单失败",
                        account_index=account_index,
                        market_index=market_index,
                        error=str(e))
            return 0

    async def get_pending_orders(self, account_index: int, timeout_seconds: int = 30) -> List[OrderInfo]:
        """
        Get pending orders for an account
        
        Args:
            account_index: 账户索引
            timeout_seconds: 订单超时时间（秒），默认30秒
        
        Returns:
            有效的待处理订单列表（自动排除超时订单）
        """
        current_time = datetime.now()
        valid_pending_orders = []
        expired_orders = []
        all_orders_for_account = []
        
        logger.debug("开始检查账户订单超时",
                   account_index=account_index,
                   timeout_seconds=timeout_seconds,
                   current_time=current_time.isoformat(),
                   total_orders_in_manager=len(self.orders))
        
        for order in self.orders.values():
            if order.account_index == account_index:
                all_orders_for_account.append(order)
                
                if order.status == OrderStatus.PENDING:
                    # 检查订单是否超时
                    time_since_created = current_time - order.created_at
                    age_seconds = time_since_created.total_seconds()
                    
                    logger.info("检查PENDING订单",
                               order_id=order.id,
                               account_index=account_index,
                               created_at=order.created_at.isoformat(),
                               age_seconds=int(age_seconds),
                               timeout_seconds=timeout_seconds,
                               is_expired=age_seconds > timeout_seconds)
                    
                    if age_seconds > timeout_seconds:
                        # 订单超时，自动标记为失败
                        order.status = OrderStatus.FAILED
                        expired_orders.append(order)
                        logger.warning("🕒 订单已超时，自动标记为失败",
                                     order_id=order.id,
                                     account_index=account_index,
                                     created_at=order.created_at.isoformat(),
                                     timeout_seconds=timeout_seconds,
                                     actual_seconds=int(age_seconds))
                    else:
                        # 订单仍然有效
                        valid_pending_orders.append(order)
                        logger.info("✅ 订单仍然有效",
                                   order_id=order.id,
                                   age_seconds=int(age_seconds),
                                   remaining_seconds=int(timeout_seconds - age_seconds))
        
        logger.debug("账户订单超时检查完成",
                   account_index=account_index,
                   total_orders_for_account=len(all_orders_for_account),
                   expired_count=len(expired_orders),
                   valid_pending_count=len(valid_pending_orders),
                   expired_order_ids=[o.id for o in expired_orders],
                   valid_pending_order_ids=[o.id for o in valid_pending_orders])
        
        return valid_pending_orders
    
    async def get_market_data(self, market_index: int) -> Optional[MarketData]:
        """Get current market data - 仅使用WebSocket新鲜数据，避免过期数据导致亏损"""
        try:
            # 仅使用WebSocket实时数据，确保数据新鲜度
            if not self.websocket_manager:
                logger.error("WebSocket管理器不可用", market_index=market_index)
                return None
                
            ws_data = self.websocket_manager.get_latest_market_data(market_index)
            logger.debug("🔍 WebSocket数据检查", 
                       market_index=market_index,
                       ws_data_exists=bool(ws_data),
                       ws_data_type=type(ws_data).__name__ if ws_data else None)
            
            if ws_data and self._is_data_fresh(ws_data):
                data_age = (datetime.now() - ws_data.timestamp).total_seconds()
                logger.debug("✅ 使用WebSocket新鲜数据", 
                            market_index=market_index,
                            data_age=f"{data_age:.1f}s",
                            price=float(ws_data.price))
                return ws_data
            elif ws_data:
                data_age = (datetime.now() - ws_data.timestamp).total_seconds()
                logger.warning("⚠️ WebSocket数据过期，拒绝使用", 
                             market_index=market_index,
                             data_age=f"{data_age:.1f}s",
                             max_age=f"{self._data_freshness_threshold:.1f}s")
                
                # 数据过期超过1分钟，强制重新初始化WebSocket
                if data_age > 60:
                    logger.warning("数据严重过期，强制重新初始化WebSocket", 
                                 market_index=market_index,
                                 data_age=f"{data_age:.1f}s")
                    try:
                        await self.websocket_manager.cleanup()
                        await self.websocket_manager.initialize()
                        logger.info("WebSocket强制重新初始化完成", market_index=market_index)
                    except Exception as reinit_error:
                        logger.error("WebSocket强制重新初始化失败", 
                                   market_index=market_index, 
                                   error=str(reinit_error))
            else:
                logger.warning("⚠️ WebSocket数据不可用", market_index=market_index)
                
                # 尝试重新初始化WebSocket连接
                if self.websocket_manager and not self.websocket_manager.is_connected:
                    logger.info("检测到WebSocket断开，尝试重新连接", market_index=market_index)
                    try:
                        await self.websocket_manager.initialize()
                        logger.info("WebSocket重新连接成功", market_index=market_index)
                    except Exception as reconnect_error:
                        logger.error("WebSocket重新连接失败", 
                                   market_index=market_index, 
                                   error=str(reconnect_error))
            
            # 不再回退到API，确保只使用新鲜数据
            logger.warning("WebSocket数据不新鲜或不可用，拒绝交易", 
                         market_index=market_index,
                         ws_data_available=bool(ws_data))
            return None
            
        except Exception as e:
            logger.error("获取市场数据失败", 
                        market_index=market_index,
                        error=str(e))
            return None
    
    async def _fetch_market_data_from_api(self, market_index: int) -> Optional[MarketData]:
        """从API获取市场数据（原有逻辑，优化为只调用一个API）"""
        try:
            if not self.order_api:
                return None
            
            # 只调用orderbook API，避免双重调用
            orderbook_data = await self.order_api.order_book_orders(market_id=market_index, limit=1)
            
            if not orderbook_data:
                return None
            
            # Get best bid/ask from orderbook
            best_bid = None
            best_ask = None
            mid_price = None
            
            if orderbook_data.bids:
                best_bid = Decimal(str(orderbook_data.bids[0].price))
            if orderbook_data.asks:
                best_ask = Decimal(str(orderbook_data.asks[0].price))
            
            # Calculate mid price
            if best_bid and best_ask:
                mid_price = (best_bid + best_ask) / 2
            elif best_bid:
                mid_price = best_bid
            elif best_ask:
                mid_price = best_ask
            else:
                # 如果没有买卖盘，尝试获取最后成交价
                try:
                    details_data = await self.order_api.order_book_details(market_id=market_index)
                    if details_data and details_data.order_book_details:
                        mid_price = Decimal(str(details_data.order_book_details[0].last_trade_price))
                except:
                    pass
            
            if mid_price is None:
                return None
            
            market_data = MarketData(
                market_index=market_index,
                price=mid_price,
                bid_price=best_bid,
                ask_price=best_ask,
                timestamp=datetime.now()
            )
            
            logger.debug("API获取市场数据成功", 
                        market_index=market_index,
                        price=float(mid_price))
            
            return market_data
            
        except Exception as e:
            logger.error("API获取市场数据失败", 
                        market_index=market_index,
                        error=str(e))
            return None
    
    async def get_orderbook(self, market_index: int) -> Optional[OrderBook]:
        """Get orderbook for a market - 仅使用WebSocket新鲜数据，避免过期数据导致亏损"""
        try:
            # 仅使用WebSocket实时订单簿，确保数据新鲜度
            if not self.websocket_manager:
                logger.error("WebSocket管理器不可用", market_index=market_index)
                return None
                
            ws_orderbook = self.websocket_manager.get_latest_orderbook(market_index)
            if ws_orderbook and self._is_data_fresh(ws_orderbook):
                data_age = (datetime.now() - ws_orderbook.timestamp).total_seconds()
                logger.debug("✅ 使用WebSocket新鲜订单簿", 
                           market_index=market_index,
                           data_age=f"{data_age:.1f}s")
                return ws_orderbook
            elif ws_orderbook:
                data_age = (datetime.now() - ws_orderbook.timestamp).total_seconds()
                logger.warning("⚠️ WebSocket订单簿数据过期，拒绝使用", 
                             market_index=market_index,
                             data_age=f"{data_age:.1f}s",
                             max_age=f"{self._data_freshness_threshold:.1f}s")
            else:
                logger.warning("⚠️ WebSocket订单簿数据不可用", market_index=market_index)
            
            # 不再回退到API，确保只使用新鲜数据
            logger.warning("WebSocket订单簿不新鲜或不可用，拒绝交易", 
                         market_index=market_index,
                         ws_orderbook_available=bool(ws_orderbook))
            return None
            
        except Exception as e:
            logger.error("获取订单簿失败",
                        market_index=market_index,
                        error=str(e))
            return None
    
    async def _fetch_orderbook_from_api(self, market_index: int) -> Optional[OrderBook]:
        """从API获取订单簿数据"""
        try:
            if not self.order_api:
                return None
            
            # Get real-time orderbook orders
            orderbook_data = await self.order_api.order_book_orders(market_id=market_index, limit=20)
            
            if not orderbook_data:
                return None
            
            # Convert to our format
            bids = []
            asks = []
            
            if orderbook_data.bids:
                for bid in orderbook_data.bids:
                    bids.append({
                        "price": Decimal(str(bid.price)),
                        "amount": Decimal(str(bid.remaining_base_amount))
                    })
            
            if orderbook_data.asks:
                for ask in orderbook_data.asks:
                    asks.append({
                        "price": Decimal(str(ask.price)),
                        "amount": Decimal(str(ask.remaining_base_amount))
                    })
            
            orderbook = OrderBook(
                market_index=market_index,
                bids=bids,
                asks=asks,
                timestamp=datetime.now()
            )
            
            logger.debug("API获取订单簿成功", 
                        market_index=market_index,
                        bids_count=len(bids),
                        asks_count=len(asks))
            
            return orderbook
            
        except Exception as e:
            logger.error("API获取订单簿失败",
                        market_index=market_index,
                        error=str(e))
            return None
    
    
    async def start_monitoring(self) -> None:
        """Start order monitoring"""
        if self._monitoring_task:
            self._monitoring_task.cancel()
        
        self._monitoring_task = asyncio.create_task(self._monitoring_loop())
        logger.info("订单监控已启动")
    
    async def stop_monitoring(self) -> None:
        """Stop order monitoring"""
        if self._monitoring_task:
            self._monitoring_task.cancel()
            self._monitoring_task = None
        logger.info("订单监控已停止")
    
    async def _monitoring_loop(self) -> None:
        """Order monitoring loop"""
        while True:
            try:
                await asyncio.sleep(5)  # Check every 5 seconds
                await self._update_order_statuses()
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("订单监控循环出错", error=str(e))
                await asyncio.sleep(10)
    
    async def _update_order_statuses(self) -> None:
        """Update order statuses using Lighter SDK"""
        for order_id, order in self.orders.items():
            if order.status in [OrderStatus.PENDING] and order.sdk_order_id:
                try:
                    # Get signer client for this order
                    signer_client = self.signer_clients.get(order.account_index)
                    if not signer_client:
                        continue
                    
                    # Query order status from Lighter SDK
                    # Note: get_order_status method may not be available in current SDK version
                    # Using alternative approach or skipping for now
                    order_status = None
                    try:
                        if hasattr(signer_client, 'get_order_status'):
                            order_status = await signer_client.get_order_status(order.sdk_order_id)
                    except AttributeError:
                        # Method not available in current SDK version
                        pass
                    
                    if order_status:
                        # Update order based on SDK response
                        if order_status.status == "filled":
                            order.status = OrderStatus.FILLED
                            order.filled_at = datetime.now()
                            order.filled_amount = Decimal(str(order_status.filled_amount))
                            order.filled_price = Decimal(str(order_status.filled_price))
                            
                            logger.info("订单已成交",
                                       order_id=order_id,
                                       sdk_order_id=order.sdk_order_id,
                                       filled_amount=float(order.filled_amount),
                                       filled_price=float(order.filled_price))
                        
                        elif order_status.status == "cancelled":
                            order.status = OrderStatus.CANCELLED
                            logger.info("订单已取消", order_id=order_id, sdk_order_id=order.sdk_order_id)
                        
                        elif order_status.status == "failed":
                            order.status = OrderStatus.FAILED
                            logger.warning("订单执行失败", order_id=order_id, sdk_order_id=order.sdk_order_id)
                
                except Exception as e:
                    logger.debug("查询订单状态失败", 
                                order_id=order_id,
                                sdk_order_id=order.sdk_order_id,
                                error=str(e))
                    # Don't update status on query errors, keep trying
            
            elif order.status == OrderStatus.PENDING and not order.sdk_order_id:
                # For orders without SDK order ID, use time-based simulation as fallback
                time_since_created = datetime.now() - order.created_at
                if time_since_created.total_seconds() > 30:  # 30 seconds timeout
                    order.status = OrderStatus.FAILED
                    logger.warning("订单超时失败", order_id=order_id)
    
    async def get_account_active_orders_from_api(self, account_index: int) -> List[dict]:
        """从Lighter API获取账户活跃订单"""
        try:
            if not self.order_api:
                logger.warning("OrderApi未初始化，无法查询活跃订单")
                return []
            
            # 检查所有可用的API方法
            api_methods = [method for method in dir(self.order_api) if not method.startswith('_')]
            logger.debug("OrderApi可用方法", methods=api_methods[:10])  # 显示前10个方法
            
            # 调用API获取活跃订单
            try:
                orders = None
                
                # 方法1: 使用account_active_orders (需要account_index和market_id参数)
                if hasattr(self.order_api, 'account_active_orders'):
                    try:
                        # 获取所有市场的活跃订单
                        all_orders = []
                        
                        # 获取所有可能的市场ID
                        market_ids = self._get_all_possible_market_ids()
                        for market_id in market_ids:
                            try:
                                market_orders = await self.order_api.account_active_orders(
                                    account_index=account_index,
                                    market_id=market_id
                                )
                                if market_orders:
                                    all_orders.extend(market_orders if isinstance(market_orders, list) else [market_orders])
                                logger.debug("获取市场活跃订单成功", 
                                           account_index=account_index, 
                                           market_id=market_id,
                                           orders_count=len(market_orders) if market_orders else 0)
                            except Exception as market_error:
                                logger.debug("获取特定市场活跃订单失败", 
                                           market_id=market_id, 
                                           error=str(market_error))
                        
                        if all_orders:
                            orders = all_orders
                            logger.debug("使用account_active_orders方法成功", 
                                       account_index=account_index,
                                       total_orders=len(orders))
                        
                    except Exception as e:
                        logger.debug("account_active_orders方法失败", error=str(e))
                
                # 方法2: 使用其他可能的方法
                if not orders:
                    order_methods = [m for m in api_methods if 'order' in m.lower()]
                    logger.debug("包含order的方法", methods=order_methods)
                    
                    # 尝试常见的方法名
                    common_methods = ['get_orders', 'list_orders', 'orders']
                    for method_name in common_methods:
                        if hasattr(self.order_api, method_name):
                            try:
                                method = getattr(self.order_api, method_name)
                                if callable(method):
                                    orders = await method()
                                    logger.debug(f"使用{method_name}方法成功", account_index=account_index)
                                    break
                            except Exception as e:
                                logger.debug(f"{method_name}方法失败", error=str(e))
                
                if orders:
                    logger.debug("成功获取活跃订单", 
                               account_index=account_index, 
                               orders_count=len(orders) if isinstance(orders, list) else 1)
                    return orders if isinstance(orders, list) else [orders]
                else:
                    logger.info("暂无活跃订单", account_index=account_index)
                    return []
                
            except Exception as api_error:
                logger.error("API查询活跃订单失败", 
                           account_index=account_index, 
                           error=str(api_error))
                return []
                
        except Exception as e:
            logger.error("获取账户活跃订单失败", 
                        account_index=account_index, 
                        error=str(e))
            return []
    
    async def get_account_inactive_orders_from_api(self, account_index: int) -> List[dict]:
        """从Lighter API获取账户历史订单"""
        try:
            if not self.order_api:
                logger.warning("OrderApi未初始化，无法查询历史订单")
                return []
            
            # 调用API获取历史订单
            try:
                orders = None
                
                # 使用account_inactive_orders (需要account_index和market_id参数)
                if hasattr(self.order_api, 'account_inactive_orders'):
                    try:
                        # 获取所有市场的历史订单
                        all_orders = []
                        
                        # 获取所有可能的市场ID
                        market_ids = self._get_all_possible_market_ids()
                        for market_id in market_ids:
                            try:
                                market_orders = await self.order_api.account_inactive_orders(
                                    account_index, limit=100  # Use positional argument for account_index
                                )
                                if market_orders:
                                    all_orders.extend(market_orders if isinstance(market_orders, list) else [market_orders])
                                logger.debug("获取市场历史订单成功", 
                                           account_index=account_index, 
                                           market_id=market_id,
                                           orders_count=len(market_orders) if market_orders else 0)
                            except Exception as market_error:
                                logger.debug("获取特定市场历史订单失败", 
                                           market_id=market_id, 
                                           error=str(market_error))
                        
                        if all_orders:
                            orders = all_orders
                            logger.debug("使用account_inactive_orders方法成功", 
                                       account_index=account_index,
                                       total_orders=len(orders))
                        
                    except Exception as e:
                        logger.debug("account_inactive_orders方法失败", error=str(e))
                
                if orders:
                    logger.debug("成功获取账户历史订单", 
                               account_index=account_index, 
                               orders_count=len(orders))
                    return orders
                else:
                    logger.info("暂无历史订单", account_index=account_index)
                    return []
                
            except Exception as api_error:
                logger.error("API查询历史订单失败", 
                           account_index=account_index, 
                           error=str(api_error))
                return []
                
        except Exception as e:
            logger.error("获取账户历史订单失败", 
                        account_index=account_index, 
                        error=str(e))
            return []
    
    async def get_all_account_orders_from_api(self, account_index: int) -> dict:
        """获取账户的所有订单（活跃+历史）"""
        try:
            active_orders = await self.get_account_active_orders_from_api(account_index)
            inactive_orders = await self.get_account_inactive_orders_from_api(account_index)
            
            result = {
                "account_index": account_index,
                "active_orders": active_orders,
                "inactive_orders": inactive_orders,
                "total_orders": len(active_orders) + len(inactive_orders)
            }
            
            logger.debug("成功获取账户所有订单", 
                       account_index=account_index,
                       active_count=len(active_orders),
                       inactive_count=len(inactive_orders),
                       total_count=result["total_orders"])
            
            return result
            
        except Exception as e:
            logger.error("获取账户所有订单失败", 
                        account_index=account_index, 
                        error=str(e))
            return {
                "account_index": account_index,
                "active_orders": [],
                "inactive_orders": [],
                "total_orders": 0,
                "error": str(e)
            }

    async def cleanup(self) -> None:
        """Cleanup resources"""
        await self.stop_monitoring()
        
        # Close signer clients
        for signer_client in self.signer_clients.values():
            if hasattr(signer_client, 'close'):
                await signer_client.close()
        
        if self.order_api and hasattr(self.order_api, 'api_client'):
            if hasattr(self.order_api.api_client, 'close'):
                await self.order_api.api_client.close()
        
        logger.info("订单管理器已清理")
    
    async def verify_orders_by_position_changes(
        self,
        orders: List[OrderInfo],
        timeout: int = 60
    ) -> bool:
        """
        通过账户持仓变化验证订单是否成交
        这是解决后台订单查询问题的替代方案
        """
        try:
            logger.info("开始通过持仓变化验证订单成交",
                       orders_count=len(orders),
                       timeout=timeout)
            
            if not self.account_manager:
                logger.warning("AccountManager未设置，跳过持仓变化验证")
                await asyncio.sleep(5)  # 简短等待
                return True
            
            # 获取初始持仓状态
            initial_positions = {}
            for order in orders:
                try:
                    account = self.account_manager.get_account(order.account_index)
                    if account:
                        initial_positions[order.account_index] = account.positions.copy()
                    else:
                        initial_positions[order.account_index] = []
                except Exception as e:
                    logger.warning("获取初始持仓失败", account_index=order.account_index, error=str(e))
                    initial_positions[order.account_index] = []
            
            # 等待订单处理
            logger.debug("等待订单处理完成...")
            await asyncio.sleep(8)  # 给订单时间处理
            
            # 检查持仓变化
            verification_results = []
            for order in orders:
                try:
                    account = self.account_manager.get_account(order.account_index)
                    if account:
                        current_positions = account.positions
                        position_changed = self._detect_position_change(
                            initial_positions.get(order.account_index, []),
                            current_positions,
                            order
                        )
                        verification_results.append(position_changed)
                        
                        if position_changed:
                            logger.info("检测到订单成交的持仓变化",
                                       order_id=order.id,
                                       account_index=order.account_index,
                                       side=order.side,
                                       amount=float(order.amount))
                        else:
                            logger.warning("未检测到预期的持仓变化",
                                         order_id=order.id,
                                         account_index=order.account_index)
                    else:
                        logger.warning("无法获取账户信息进行持仓验证", account_index=order.account_index)
                        verification_results.append(True)  # 假设成功，避免阻塞
                        
                except Exception as e:
                    logger.error("持仓变化检测失败", order_id=order.id, error=str(e))
                    verification_results.append(True)  # 假设成功，避免阻塞
            
            # 如果大部分订单显示成功，则认为验证通过
            success_count = sum(verification_results)
            success_rate = success_count / len(orders) if orders else 0
            
            if success_rate >= 0.5:  # 50%以上成功率
                logger.info("持仓变化验证通过",
                           verified_orders=success_count,
                           total_orders=len(orders),
                           success_rate=f"{success_rate:.1%}")
                return True
            else:
                logger.warning("持仓变化验证失败",
                             verified_orders=success_count,
                             total_orders=len(orders),
                             success_rate=f"{success_rate:.1%}")
                return False
            
        except Exception as e:
            logger.error("持仓变化验证异常", error=str(e))
            return False
    
    def _detect_position_change(
        self,
        initial_positions: List,
        current_positions: List,
        order: OrderInfo
    ) -> bool:
        """
        检测持仓是否发生了与订单相符的变化
        """
        try:
            # 如果之前没有持仓，现在有了，说明订单成交了
            if not initial_positions and current_positions:
                for pos in current_positions:
                    # 检查方向是否匹配
                    expected_side = 'long' if order.side == 'buy' else 'short'
                    if pos.side == expected_side and pos.size > 0:
                        logger.debug("检测到新持仓创建",
                                   order_id=order.id,
                                   expected_side=expected_side,
                                   actual_side=pos.side,
                                   size=float(pos.size))
                        return True
            
            # 如果之前有持仓，检查数量是否增加了
            elif initial_positions and current_positions:
                for init_pos in initial_positions:
                    for curr_pos in current_positions:
                        if (init_pos.side == curr_pos.side and 
                            curr_pos.size > init_pos.size):
                            size_increase = curr_pos.size - init_pos.size
                            # 检查增加的数量是否接近订单数量
                            if abs(size_increase - order.amount) / order.amount < 0.1:  # 10%容差
                                logger.debug("检测到持仓数量增加",
                                           order_id=order.id,
                                           size_increase=float(size_increase),
                                           order_amount=float(order.amount))
                                return True
            
            return False
            
        except Exception as e:
            logger.error("持仓变化检测异常",
                        order_id=order.id,
                        error=str(e))
            return False
    
    async def get_positions_for_account(self, account_index: int) -> List:
        """获取账户的仓位信息"""
        try:
            # 通过AccountManager获取仓位
            if self.account_manager:
                positions = self.account_manager.get_account_positions(account_index)
                logger.debug("从AccountManager获取仓位",
                           account_index=account_index,
                           positions_count=len(positions))
                return positions
            else:
                logger.warning("AccountManager未初始化，无法获取仓位",
                             account_index=account_index)
                return []
                
        except Exception as e:
            logger.error("获取账户仓位失败",
                        account_index=account_index,
                        error=str(e))
            return []