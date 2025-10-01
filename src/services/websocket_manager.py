"""
WebSocket Manager for Real-time Updates from Lighter Protocol
Based on official lighter-python examples
"""

import asyncio
import json
from typing import Dict, List, Optional, Callable, Set
from datetime import datetime
from decimal import Decimal
import structlog

import lighter
from src.models import MarketData, OrderBook
from src.config.config_manager import ConfigManager
from src.core.lighter_client_factory import get_client_factory

logger = structlog.get_logger()


class WebSocketManager:
    """Manages WebSocket connections for real-time market data using official Lighter WsClient"""
    
    def __init__(self, config_manager: ConfigManager):
        self.config_manager = config_manager
        self.client_factory = get_client_factory(config_manager)
        self.ws_client: Optional[lighter.WsClient] = None
        self.is_connected = False
        
        # 从配置中获取所有启用的交易对市场索引
        enabled_markets = self._get_enabled_markets_from_config()
        self.subscribed_markets: Set[int] = set(enabled_markets)
        self.subscribed_accounts: Set[int] = set()
        
        # Callbacks for different data types
        self.market_data_callbacks: List[Callable[[MarketData], None]] = []
        self.orderbook_callbacks: List[Callable[[OrderBook], None]] = []
        self.account_callbacks: List[Callable[[dict], None]] = []
        
        # Latest data cache
        self.latest_market_data: Dict[int, MarketData] = {}
        self.latest_orderbooks: Dict[int, OrderBook] = {}
        self.latest_account_data: Dict[int, dict] = {}
        
        self._client_task: Optional[asyncio.Task] = None
        
        # Connection parameters
        self.reconnect_interval = 5  # seconds
        self.max_reconnect_attempts = 10
        self.current_reconnect_attempts = 0
    
    def _get_enabled_markets_from_config(self) -> List[int]:
        """从配置中获取所有启用的交易对市场索引"""
        try:
            trading_pairs = self.config_manager.get_trading_pairs()
            enabled_markets = []
            
            for pair in trading_pairs:
                if pair.is_enabled:
                    enabled_markets.append(pair.market_index)
                    logger.info("发现启用的交易对配置", 
                               pair_name=pair.name, 
                               pair_id=pair.id,
                               market_index=pair.market_index)
            
            if not enabled_markets:
                logger.warning("未找到启用的交易对配置，WebSocket将不订阅任何市场")
                return []
            
            return enabled_markets
            
        except Exception as e:
            logger.error("获取启用市场配置失败", error=str(e))
            return []
        
    async def initialize(self) -> None:
        """Initialize WebSocket manager using official Lighter WsClient"""
        try:
            logger.info("初始化WebSocket管理器...")
            
            # 使用配置中的启用市场进行订阅
            markets_to_subscribe = list(self.subscribed_markets)
            
            logger.info("订阅启用的市场", markets=markets_to_subscribe)
            
            self.ws_client = self.client_factory.create_ws_client(
                order_book_ids=markets_to_subscribe,
                account_ids=[],  # Start empty for accounts
                on_order_book_update=self._on_order_book_update,
                on_account_update=self._on_account_update,
            )
            
            # Start the WebSocket client
            self._client_task = asyncio.create_task(self._run_client())
            
            logger.info("WebSocket管理器初始化完成", client_type="lighter.WsClient")
            
        except Exception as e:
            logger.error("WebSocket管理器初始化失败", error=str(e))
            raise
    
    def _get_websocket_url(self) -> str:
        """Get WebSocket URL from configuration"""
        # Use the configured WebSocket URL directly
        if hasattr(self.config_manager, 'get_ws_url'):
            return self.config_manager.get_ws_url()
        
        # Fallback: get from config data directly
        config_data = self.config_manager.config
        if config_data and 'global' in config_data and 'ws_url' in config_data['global']:
            return config_data['global']['ws_url']
        
        # Last fallback: construct from API URL
        api_url = self.config_manager.get_api_url()
        if api_url.startswith("https://"):
            ws_url = api_url.replace("https://", "wss://") + "/stream"
        elif api_url.startswith("http://"):
            ws_url = api_url.replace("http://", "ws://") + "/stream"
        else:
            ws_url = f"wss://{api_url}/stream"
        
        return ws_url
    
    async def _run_client(self) -> None:
        """Run the WebSocket client with auto-reconnect"""
        attempt = 0
        while attempt < self.max_reconnect_attempts:
            try:
                logger.info("启动WebSocket客户端", attempt=attempt + 1)
                self.is_connected = True
                
                # Run the client (this will block until connection fails)
                await self.ws_client.run_async()
                
            except Exception as e:
                attempt += 1
                self.is_connected = False
                logger.error("WebSocket连接失败", 
                           error=str(e), 
                           attempt=attempt,
                           max_attempts=self.max_reconnect_attempts)
                
                if attempt < self.max_reconnect_attempts:
                    logger.info("等待重连...", 
                               delay=self.reconnect_interval,
                               attempt=attempt + 1)
                    await asyncio.sleep(self.reconnect_interval)
                else:
                    logger.error("达到最大重连次数，停止重连")
                    break
    
    def _on_order_book_update(self, market_id, order_book_data) -> None:
        """Handle order book updates from Lighter WsClient - 匹配官方SDK回调签名"""
        try:
            logger.debug("收到订单簿更新", 
                       market_id=market_id, 
                       data_type=type(order_book_data).__name__,
                       data_keys=list(order_book_data.keys()) if isinstance(order_book_data, dict) else "not_dict")
            
            # 确保market_index是整数类型
            market_index = int(market_id) if isinstance(market_id, str) else market_id
            
            # Convert to our OrderBook format
            bids = []
            asks = []
            
            if isinstance(order_book_data, dict):
                logger.debug("订单簿数据结构调试",
                           market_id=market_id,
                           data_keys=list(order_book_data.keys()),
                           bids_exists='bids' in order_book_data,
                           asks_exists='asks' in order_book_data,
                           bids_type=type(order_book_data.get('bids', None)).__name__,
                           asks_type=type(order_book_data.get('asks', None)).__name__)
                
                if 'bids' in order_book_data:
                    raw_bids = order_book_data['bids']
                    logger.debug("Bids数据详情",
                               bids_count=len(raw_bids) if hasattr(raw_bids, '__len__') else 'no_len',
                               first_bid_type=type(raw_bids[0]).__name__ if raw_bids else 'empty',
                               first_bid_data=str(raw_bids[0])[:100] if raw_bids else 'empty')
                    
                    for bid in raw_bids:
                        if isinstance(bid, dict):
                            bids.append({
                                "price": Decimal(str(bid.get('price', '0'))),
                                "amount": Decimal(str(bid.get('size', '0')))  # 修复：使用'size'而不是'amount'
                            })
                        else:
                            logger.warning("Bid数据格式异常", bid_type=type(bid).__name__, bid_data=str(bid))
                
                if 'asks' in order_book_data:
                    raw_asks = order_book_data['asks']
                    logger.debug("Asks数据详情",
                               asks_count=len(raw_asks) if hasattr(raw_asks, '__len__') else 'no_len',
                               first_ask_type=type(raw_asks[0]).__name__ if raw_asks else 'empty',
                               first_ask_data=str(raw_asks[0])[:100] if raw_asks else 'empty')
                    
                    for ask in raw_asks:
                        if isinstance(ask, dict):
                            asks.append({
                                "price": Decimal(str(ask.get('price', '0'))),
                                "amount": Decimal(str(ask.get('size', '0')))  # 修复：使用'size'而不是'amount'
                            })
                        else:
                            logger.warning("Ask数据格式异常", ask_type=type(ask).__name__, ask_data=str(ask))
            
            orderbook = OrderBook(
                market_index=market_index,
                bids=bids,
                asks=asks,
                timestamp=datetime.now()
            )
            
            # Cache latest orderbook
            self.latest_orderbooks[market_index] = orderbook
            
            # Create market data from orderbook
            if bids and asks:
                bid_price = bids[0]["price"]
                ask_price = asks[0]["price"]
                mid_price = (bid_price + ask_price) / 2
                
                market_data = MarketData(
                    market_index=market_index,
                    price=mid_price,
                    bid_price=bid_price,
                    ask_price=ask_price,
                    timestamp=datetime.now()
                )
                
                # Cache latest market data
                self.latest_market_data[market_index] = market_data
                
                logger.debug("✅ WebSocket市场数据已更新", 
                           market_index=market_index,
                           price=float(mid_price),
                           bid=float(bid_price),
                           ask=float(ask_price))
                
                # Notify market data callbacks
                for callback in self.market_data_callbacks:
                    try:
                        callback(market_data)
                    except Exception as e:
                        logger.error("市场数据回调失败", error=str(e))
            
            # Notify orderbook callbacks
            for callback in self.orderbook_callbacks:
                try:
                    callback(orderbook)
                except Exception as e:
                    logger.error("订单簿回调失败", error=str(e))
                        
        except Exception as e:
            logger.error("处理订单簿更新失败", 
                        market_id=market_id,
                        error=str(e),
                        order_book_type=type(order_book_data).__name__)
    
    def _on_account_update(self, account_id, account_data) -> None:
        """Handle account updates from Lighter WsClient - 匹配官方SDK回调签名"""
        try:
            logger.debug("收到账户更新", 
                       account_id=account_id,
                       data_type=type(account_data).__name__)
            
            # 确保account_index是整数类型
            account_index = int(account_id) if isinstance(account_id, str) else account_id
            
            if isinstance(account_data, dict):
                # Cache latest account data
                self.latest_account_data[account_index] = account_data
                
                # Notify account callbacks
                for callback in self.account_callbacks:
                    try:
                        callback(account_data)
                    except Exception as e:
                        logger.error("账户数据回调失败", error=str(e))
                
                logger.debug("账户更新处理完成",
                           account_index=account_index,
                           balance=account_data.get('balance'))
                            
        except Exception as e:
            logger.error("处理账户更新失败", 
                        account_id=account_id,
                        error=str(e))
    
    async def subscribe_account(self, account_index: int) -> None:
        """Subscribe to account updates"""
        try:
            self.subscribed_accounts.add(account_index)
            
            # Update the WebSocket client with new subscriptions
            if self.ws_client:
                await self._update_subscriptions()
            
            logger.info("订阅账户数据",
                       account_index=account_index,
                       total_accounts=len(self.subscribed_accounts))
                       
        except Exception as e:
            logger.error("订阅账户数据失败",
                        account_index=account_index,
                        error=str(e))
    
    async def _update_subscriptions(self) -> None:
        """Update WebSocket client subscriptions"""
        try:
            # Cancel current client task
            if self._client_task and not self._client_task.done():
                self._client_task.cancel()
                try:
                    await self._client_task
                except asyncio.CancelledError:
                    pass
            
            # Create new client with updated subscriptions
            self.ws_client = self.client_factory.create_ws_client(
                order_book_ids=list(self.subscribed_markets),
                account_ids=list(self.subscribed_accounts),
                on_order_book_update=self._on_order_book_update,
                on_account_update=self._on_account_update,
            )
            
            # Start new client task
            self._client_task = asyncio.create_task(self._run_client())
            
            logger.info("更新WebSocket订阅",
                       markets=len(self.subscribed_markets),
                       accounts=len(self.subscribed_accounts))
                       
        except Exception as e:
            logger.error("更新订阅失败", error=str(e))
    
    async def _maintain_connection(self, ws_url: str) -> None:
        """Maintain WebSocket connection with auto-reconnect"""
        while True:
            try:
                logger.info("正在连接WebSocket...", url=ws_url)
                
                async with websockets.connect(
                    ws_url,
                    ping_interval=20,
                    ping_timeout=10,
                    close_timeout=10
                ) as websocket:
                    self.websocket = websocket
                    self.is_connected = True
                    self.current_reconnect_attempts = 0
                    
                    logger.info("WebSocket连接建立成功", url=ws_url)
                    
                    # Start heartbeat
                    self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
                    
                    # Re-subscribe to markets if any
                    if self.subscribed_markets:
                        await self._resubscribe_markets()
                    
                    # Handle incoming messages
                    await self._message_handler()
                    
            except websockets.exceptions.ConnectionClosed:
                logger.warning("WebSocket连接关闭")
            except Exception as e:
                logger.error("WebSocket连接失败", error=str(e))
            
            finally:
                self.is_connected = False
                self.websocket = None
                
                if self._heartbeat_task:
                    self._heartbeat_task.cancel()
                    self._heartbeat_task = None
                
                # Reconnect logic
                self.current_reconnect_attempts += 1
                if self.current_reconnect_attempts <= self.max_reconnect_attempts:
                    logger.info("等待重连...",
                               attempt=self.current_reconnect_attempts,
                               max_attempts=self.max_reconnect_attempts,
                               delay=self.reconnect_interval)
                    await asyncio.sleep(self.reconnect_interval)
                else:
                    logger.error("达到最大重连次数，停止重连")
                    break
    
    async def _heartbeat_loop(self) -> None:
        """Send periodic heartbeat to keep connection alive"""
        while self.is_connected and self.websocket:
            try:
                await asyncio.sleep(30)  # Send heartbeat every 30 seconds
                if self.websocket and not self.websocket.closed:
                    await self.websocket.ping()
                    logger.debug("发送WebSocket心跳")
            except Exception as e:
                logger.warning("心跳发送失败", error=str(e))
                break
    
    async def _message_handler(self) -> None:
        """Handle incoming WebSocket messages"""
        try:
            async for message in self.websocket:
                try:
                    data = json.loads(message)
                    await self._process_message(data)
                except json.JSONDecodeError:
                    logger.warning("收到无效JSON消息", message=message[:100])
                except Exception as e:
                    logger.error("处理消息失败", error=str(e), message=message[:100])
                    
        except websockets.exceptions.ConnectionClosed:
            logger.info("WebSocket消息循环结束：连接关闭")
        except Exception as e:
            logger.error("WebSocket消息处理循环失败", error=str(e))
    
    async def _process_message(self, data: dict) -> None:
        """Process incoming message based on type"""
        message_type = data.get("type")
        
        if message_type == "orderbook":
            await self._handle_orderbook_update(data)
        elif message_type == "ticker" or message_type == "price":
            await self._handle_price_update(data)
        elif message_type == "trade":
            await self._handle_trade_update(data)
        elif message_type == "error":
            logger.error("WebSocket错误消息", error=data.get("message"))
        elif message_type == "subscribed":
            logger.info("订阅成功", market=data.get("market_index"))
        else:
            logger.debug("未知消息类型", type=message_type, data=data)
    
    async def _handle_orderbook_update(self, data: dict) -> None:
        """Handle orderbook update"""
        try:
            market_index = data.get("market_index")
            if not market_index:
                return
            
            # Parse orderbook data
            bids = []
            asks = []
            
            for bid_data in data.get("bids", []):
                bids.append({
                    "price": Decimal(str(bid_data.get("price", "0"))),
                    "amount": Decimal(str(bid_data.get("amount", "0")))
                })
            
            for ask_data in data.get("asks", []):
                asks.append({
                    "price": Decimal(str(ask_data.get("price", "0"))),
                    "amount": Decimal(str(ask_data.get("amount", "0")))
                })
            
            orderbook = OrderBook(
                market_index=market_index,
                bids=bids,
                asks=asks,
                timestamp=datetime.now()
            )
            
            # Cache latest orderbook
            self.latest_orderbooks[market_index] = orderbook
            
            # Notify callbacks
            for callback in self.orderbook_callbacks:
                try:
                    callback(orderbook)
                except Exception as e:
                    logger.error("订单簿回调失败", error=str(e))
            
            # 降低频繁的订单簿更新日志级别
            # logger.debug("订单簿更新",
            #             market_index=market_index,
            #             bids_count=len(bids),
            #             asks_count=len(asks))
                        
        except Exception as e:
            logger.error("处理订单簿更新失败", error=str(e))
    
    async def _handle_price_update(self, data: dict) -> None:
        """Handle price/ticker update"""
        try:
            market_index = data.get("market_index")
            if not market_index:
                return
            
            price = Decimal(str(data.get("price", "0")))
            bid_price = Decimal(str(data.get("bid", "0"))) if data.get("bid") else None
            ask_price = Decimal(str(data.get("ask", "0"))) if data.get("ask") else None
            
            market_data = MarketData(
                market_index=market_index,
                price=price,
                bid_price=bid_price,
                ask_price=ask_price,
                timestamp=datetime.now()
            )
            
            # Cache latest market data
            self.latest_market_data[market_index] = market_data
            
            # Notify callbacks
            for callback in self.market_data_callbacks:
                try:
                    callback(market_data)
                except Exception as e:
                    logger.error("市场数据回调失败", error=str(e))
            
            # logger.debug("价格更新",
            #             market_index=market_index,
            #             price=float(price),
            #             bid=float(bid_price) if bid_price else None,
            #             ask=float(ask_price) if ask_price else None)
                        
        except Exception as e:
            logger.error("处理价格更新失败", error=str(e))
    
    async def _handle_trade_update(self, data: dict) -> None:
        """Handle trade update"""
        try:
            market_index = data.get("market_index")
            if not market_index:
                return
            
            # Update latest price from trade
            price = Decimal(str(data.get("price", "0")))
            
            # Create market data from trade
            market_data = MarketData(
                market_index=market_index,
                price=price,
                bid_price=None,
                ask_price=None,
                timestamp=datetime.now()
            )
            
            # Update cache
            if market_index in self.latest_market_data:
                # Preserve bid/ask if available
                existing = self.latest_market_data[market_index]
                market_data.bid_price = existing.bid_price
                market_data.ask_price = existing.ask_price
            
            self.latest_market_data[market_index] = market_data
            
            # Notify callbacks
            for callback in self.market_data_callbacks:
                try:
                    callback(market_data)
                except Exception as e:
                    logger.error("交易数据回调失败", error=str(e))
            
            # logger.debug("交易更新",
            #             market_index=market_index,
            #             price=float(price),
            #             amount=data.get("amount"))
                        
        except Exception as e:
            logger.error("处理交易更新失败", error=str(e))
    
    async def subscribe_market(self, market_index: int) -> None:
        """Subscribe to market data for a specific market"""
        try:
            self.subscribed_markets.add(market_index)
            
            # Update the WebSocket client with new subscriptions
            if self.ws_client:
                # Recreate client with updated subscriptions
                await self._update_subscriptions()
            
            logger.info("订阅市场数据",
                       market_index=market_index,
                       total_markets=len(self.subscribed_markets))
                       
        except Exception as e:
            logger.error("订阅市场数据失败",
                        market_index=market_index,
                        error=str(e))
    
    async def unsubscribe_market(self, market_index: int) -> None:
        """Unsubscribe from market data"""
        try:
            if self.is_connected and self.websocket:
                unsubscribe_messages = [
                    {
                        "type": "unsubscribe",
                        "channel": "orderbook", 
                        "market_index": market_index
                    },
                    {
                        "type": "unsubscribe",
                        "channel": "ticker",
                        "market_index": market_index
                    }
                ]
                
                for message in unsubscribe_messages:
                    await self.websocket.send(json.dumps(message))
            
            self.subscribed_markets.discard(market_index)
            
            # Remove from cache
            self.latest_market_data.pop(market_index, None)
            self.latest_orderbooks.pop(market_index, None)
            
            logger.info("取消订阅市场数据", market_index=market_index)
            
        except Exception as e:
            logger.error("取消订阅失败",
                        market_index=market_index,
                        error=str(e))
    
    async def _resubscribe_markets(self) -> None:
        """Re-subscribe to all previously subscribed markets"""
        markets_to_resubscribe = self.subscribed_markets.copy()
        self.subscribed_markets.clear()
        
        for market_index in markets_to_resubscribe:
            await self.subscribe_market(market_index)
    
    def add_market_data_callback(self, callback: Callable[[MarketData], None]) -> None:
        """Add callback for market data updates"""
        self.market_data_callbacks.append(callback)
    
    def add_orderbook_callback(self, callback: Callable[[OrderBook], None]) -> None:
        """Add callback for orderbook updates"""
        self.orderbook_callbacks.append(callback)
    
    def add_account_callback(self, callback: Callable[[dict], None]) -> None:
        """Add callback for account updates"""
        self.account_callbacks.append(callback)
    
    def remove_market_data_callback(self, callback: Callable[[MarketData], None]) -> None:
        """Remove market data callback"""
        if callback in self.market_data_callbacks:
            self.market_data_callbacks.remove(callback)
    
    def remove_orderbook_callback(self, callback: Callable[[OrderBook], None]) -> None:
        """Remove orderbook callback"""
        if callback in self.orderbook_callbacks:
            self.orderbook_callbacks.remove(callback)
    
    def remove_account_callback(self, callback: Callable[[dict], None]) -> None:
        """Remove account callback"""
        if callback in self.account_callbacks:
            self.account_callbacks.remove(callback)
    
    def get_latest_market_data(self, market_index: int) -> Optional[MarketData]:
        """Get latest market data for a market"""
        result = self.latest_market_data.get(market_index)
        logger.debug("🔍 WebSocket管理器数据查询", 
                   requested_market_index=market_index,
                   requested_type=type(market_index).__name__,
                   available_keys=list(self.latest_market_data.keys()),
                   result_found=bool(result))
        return result
    
    def get_latest_orderbook(self, market_index: int) -> Optional[OrderBook]:
        """Get latest orderbook for a market"""
        return self.latest_orderbooks.get(market_index)
    
    async def cleanup(self) -> None:
        """Cleanup WebSocket resources"""
        try:
            logger.info("清理WebSocket管理器...")
            
            # Cancel client task
            if self._client_task:
                self._client_task.cancel()
                try:
                    await self._client_task
                except asyncio.CancelledError:
                    pass
            
            # Close WebSocket client
            if self.ws_client:
                # The official client should handle its own cleanup
                self.ws_client = None
            
            self.is_connected = False
            
            logger.info("WebSocket管理器清理完成")
            
        except Exception as e:
            logger.error("WebSocket管理器清理失败", error=str(e))
    
    # =================================================================
    # 添加缺失的WebSocket接口方法
    # =================================================================
    
    async def subscribe_to_market_data(self, market_index: int) -> bool:
        """订阅市场数据"""
        try:
            if market_index not in self.subscribed_markets:
                self.subscribed_markets.add(market_index)
                logger.info("订阅市场数据", market_index=market_index)
                
                # 如果WebSocket已连接，需要重新创建客户端以包含新的订阅
                if self.is_connected:
                    await self._recreate_websocket_client()
                
                return True
            else:
                logger.debug("市场已订阅", market_index=market_index)
                return True
                
        except Exception as e:
            logger.error("订阅市场数据失败", market_index=market_index, error=str(e))
            return False
    
    async def subscribe_to_account_data(self, account_index: int) -> bool:
        """订阅账户数据"""
        try:
            if account_index not in self.subscribed_accounts:
                self.subscribed_accounts.add(account_index)
                logger.info("订阅账户数据", account_index=account_index)
                
                # 如果WebSocket已连接，需要重新创建客户端以包含新的订阅
                if self.is_connected:
                    await self._recreate_websocket_client()
                
                return True
            else:
                logger.debug("账户已订阅", account_index=account_index)
                return True
                
        except Exception as e:
            logger.error("订阅账户数据失败", account_index=account_index, error=str(e))
            return False
    
    async def start_websocket_client(self) -> bool:
        """启动WebSocket客户端"""
        try:
            if self.is_connected:
                logger.debug("WebSocket客户端已连接")
                return True
                
            logger.info("启动WebSocket客户端")
            await self.initialize()
            return self.is_connected
            
        except Exception as e:
            logger.error("启动WebSocket客户端失败", error=str(e))
            return False
    
    async def stop_websocket_client(self) -> bool:
        """停止WebSocket客户端"""
        try:
            if not self.is_connected:
                logger.debug("WebSocket客户端未连接")
                return True
                
            logger.info("停止WebSocket客户端")
            await self.cleanup()
            return True
            
        except Exception as e:
            logger.error("停止WebSocket客户端失败", error=str(e))
            return False
    
    async def _recreate_websocket_client(self) -> None:
        """重新创建WebSocket客户端以更新订阅"""
        try:
            logger.debug("重新创建WebSocket客户端", 
                        markets=list(self.subscribed_markets),
                        accounts=list(self.subscribed_accounts))
            
            # 停止当前客户端
            if self._client_task:
                self._client_task.cancel()
                try:
                    await self._client_task
                except asyncio.CancelledError:
                    pass
            
            # 重新初始化
            await self._initialize_websocket()
            
        except Exception as e:
            logger.error("重新创建WebSocket客户端失败", error=str(e))