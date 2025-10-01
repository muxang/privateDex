"""
Web Server for Lighter Hedge Trading System
"""

import asyncio
from typing import Optional, Dict, Any
from datetime import datetime
import structlog
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
import uvicorn

from src.config.config_manager import ConfigManager
from src.core.hedge_trading_engine import HedgeTradingEngine
from src.models import SystemStatus

logger = structlog.get_logger()


class WebServer:
    """Web server for monitoring and control"""
    
    def __init__(self, config_manager: ConfigManager, trading_engine: HedgeTradingEngine):
        self.config_manager = config_manager
        self.trading_engine = trading_engine
        self.app = FastAPI(
            title="Lighter Hedge Trading System",
            description="Advanced hedge trading system for Lighter Protocol",
            version="1.0.0"
        )
        self.server: Optional[uvicorn.Server] = None
        self.server_task: Optional[asyncio.Task] = None
        
        self._setup_routes()
        self._setup_middleware()
    
    def _setup_middleware(self) -> None:
        """Setup middleware"""
        web_config = self.config_manager.get_web_config()
        cors_origins = web_config.get('cors_origins', ["*"])
        
        self.app.add_middleware(
            CORSMiddleware,
            allow_origins=cors_origins,
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )
    
    def _setup_routes(self) -> None:
        """Setup API routes"""
        
        @self.app.get("/", response_class=HTMLResponse)
        async def root():
            """Main dashboard page"""
            return self._get_dashboard_html()
        
        @self.app.get("/api/status")
        async def get_status():
            """Get system status"""
            try:
                status = self.trading_engine.get_system_status()
                risk_summary = self.trading_engine.risk_manager.get_risk_summary()
                
                return {
                    "status": "success",
                    "data": {
                        "system": status.dict(),
                        "risk": risk_summary,
                        "timestamp": datetime.now().isoformat()
                    }
                }
            except Exception as e:
                logger.error("获取系统状态失败", error=str(e))
                raise HTTPException(status_code=500, detail=str(e))
        
        @self.app.get("/api/positions")
        async def get_positions():
            """Get active positions with detailed information"""
            try:
                positions = []
                
                # 获取对冲仓位详细信息
                strategy = getattr(self.trading_engine, 'strategy', None)
                if strategy and hasattr(strategy, 'active_positions'):
                    for position_id, hedge_pos in strategy.active_positions.items():
                        # 获取验证状态信息
                        metadata = hedge_pos.metadata if hasattr(hedge_pos, 'metadata') else {}
                        backend_validated = metadata.get('backend_validated', None)
                        close_validated = metadata.get('close_validated', None)
                        
                        position_data = {
                            "id": position_id,
                            "pair_id": hedge_pos.pair_id,
                            "status": hedge_pos.status.value,
                            "strategy": hedge_pos.strategy.value if hasattr(hedge_pos, 'strategy') else "balanced",
                            "total_pnl": float(hedge_pos.total_pnl or 0),
                            "created_at": hedge_pos.created_at.isoformat(),
                            "updated_at": hedge_pos.updated_at.isoformat() if hedge_pos.updated_at else hedge_pos.created_at.isoformat(),
                            "positions": [],
                            "stop_loss_price": float(hedge_pos.stop_loss_price) if hasattr(hedge_pos, 'stop_loss_price') and hedge_pos.stop_loss_price else None,
                            "take_profit_price": float(hedge_pos.take_profit_price) if hasattr(hedge_pos, 'take_profit_price') and hedge_pos.take_profit_price else None,
                            "metadata": metadata,
                            # 新增验证状态字段
                            "backend_validated": backend_validated,
                            "close_validated": close_validated,
                            "validation_timestamp": metadata.get('validation_timestamp'),
                            "validation_error": metadata.get('validation_error'),
                            "price_consistency_verified": metadata.get('price_consistency_verified', False),
                            "target_leverage": metadata.get('target_leverage', 1)
                        }
                        
                        # 添加详细仓位信息
                        for pos in hedge_pos.positions:
                            # 计算更多详细信息
                            leverage = getattr(pos, 'leverage', 1)
                            margin_used = getattr(pos, 'margin_used', pos.amount * pos.entry_price if pos.entry_price else 0)
                            liquidation_price = getattr(pos, 'liquidation_price', None)
                            margin_ratio = getattr(pos, 'margin_ratio', None)
                            
                            # 计算仓位价值和盈亏比例
                            position_value = float(pos.amount * pos.current_price if pos.current_price else 0)
                            entry_value = float(pos.amount * pos.entry_price if pos.entry_price else 0)
                            pnl_percentage = 0.0
                            if entry_value > 0:
                                pnl_percentage = (float(pos.unrealized_pnl or 0) / entry_value) * 100
                            
                            position_detail = {
                                "account_index": pos.account_index,
                                "side": pos.side,
                                "amount": float(pos.amount),
                                "entry_price": float(pos.entry_price) if pos.entry_price else None,
                                "current_price": float(pos.current_price) if pos.current_price else None,
                                "unrealized_pnl": float(pos.unrealized_pnl) if pos.unrealized_pnl else 0.0,
                                "market_index": pos.market_index,
                                "leverage": leverage,
                                "margin_used": float(margin_used),
                                "position_value": position_value,
                                "entry_value": entry_value,
                                "pnl_percentage": pnl_percentage,
                                "liquidation_price": float(liquidation_price) if liquidation_price else None,
                                "margin_ratio": float(margin_ratio) if margin_ratio else None,
                                "created_at": pos.created_at.isoformat(),
                                "updated_at": pos.updated_at.isoformat() if pos.updated_at else pos.created_at.isoformat()
                            }
                            position_data["positions"].append(position_detail)
                        
                        positions.append(position_data)
                
                return {
                    "status": "success",
                    "data": positions
                }
            except Exception as e:
                logger.error("获取仓位信息失败", error=str(e))
                raise HTTPException(status_code=500, detail=str(e))
        
        @self.app.get("/api/accounts")
        async def get_accounts():
            """Get account information"""
            try:
                accounts = []
                for account in self.trading_engine.account_manager.accounts.values():
                    accounts.append({
                        "index": account.index,
                        "l1_address": account.l1_address,
                        "balance": float(account.balance),
                        "available_balance": float(account.available_balance),
                        "positions_count": len(account.positions),
                        "is_active": account.is_active,
                        "last_updated": account.last_updated.isoformat()
                    })
                
                return {
                    "status": "success",
                    "data": accounts
                }
            except Exception as e:
                logger.error("获取账户信息失败", error=str(e))
                raise HTTPException(status_code=500, detail=str(e))
        
        @self.app.get("/api/trading-pairs")
        async def get_trading_pairs():
            """Get trading pairs"""
            try:
                pairs = []
                for pair in self.trading_engine.trading_pairs.values():
                    pairs.append({
                        "id": pair.id,
                        "name": pair.name,
                        "market_index": pair.market_index,
                        "is_enabled": pair.is_enabled,
                        "leverage": pair.leverage,
                        "max_positions": pair.max_positions,
                        "cooldown_minutes": pair.cooldown_minutes,
                        "hedge_strategy": pair.hedge_strategy.value
                    })
                
                return {
                    "status": "success",
                    "data": pairs
                }
            except Exception as e:
                logger.error("获取交易对信息失败", error=str(e))
                raise HTTPException(status_code=500, detail=str(e))
        
        @self.app.post("/api/engine/start")
        async def start_engine():
            """Start trading engine"""
            try:
                if not self.trading_engine.is_running:
                    await self.trading_engine.start()
                    return {"status": "success", "message": "交易引擎已启动"}
                else:
                    return {"status": "info", "message": "交易引擎已在运行"}
            except Exception as e:
                logger.error("启动交易引擎失败", error=str(e))
                raise HTTPException(status_code=500, detail=str(e))
        
        @self.app.post("/api/engine/stop")
        async def stop_engine():
            """Stop trading engine"""
            try:
                if self.trading_engine.is_running:
                    await self.trading_engine.stop()
                    return {"status": "success", "message": "交易引擎已停止"}
                else:
                    return {"status": "info", "message": "交易引擎未在运行"}
            except Exception as e:
                logger.error("停止交易引擎失败", error=str(e))
                raise HTTPException(status_code=500, detail=str(e))
        
        @self.app.get("/api/orders")
        async def get_orders():
            """Get order information from both local cache and Lighter API"""
            try:
                local_orders = []
                api_orders = []
                order_manager = getattr(self.trading_engine, 'order_manager', None)
                
                # 获取本地缓存的订单
                if order_manager and hasattr(order_manager, 'orders'):
                    for order_id, order in order_manager.orders.items():
                        order_data = {
                            "id": order_id,
                            "account_index": order.account_index,
                            "market_index": order.market_index,
                            "order_type": order.order_type,
                            "side": order.side,
                            "amount": float(order.amount),
                            "price": float(order.price),
                            "status": order.status.value,
                            "created_at": order.created_at.isoformat(),
                            "filled_amount": float(order.filled_amount) if order.filled_amount else 0.0,
                            "filled_price": float(order.filled_price) if order.filled_price else None,
                            "filled_at": order.filled_at.isoformat() if order.filled_at else None,
                            "cancelled_at": order.cancelled_at.isoformat() if hasattr(order, 'cancelled_at') and order.cancelled_at else None,
                            "sdk_order_id": order.sdk_order_id,
                            "metadata": order.metadata if hasattr(order, 'metadata') else {},
                            "source": "local_cache"
                        }
                        local_orders.append(order_data)
                
                # 获取API中的订单历史
                if order_manager:
                    try:
                        # 获取所有活跃账户的订单
                        config_manager = getattr(order_manager, 'config_manager', None)
                        if config_manager:
                            active_accounts = config_manager.get_active_accounts()
                            
                            for account in active_accounts:
                                try:
                                    # 获取账户的所有订单（活跃+历史）
                                    account_orders = await order_manager.get_all_account_orders_from_api(account.index)
                                    
                                    # 处理活跃订单
                                    for api_order in account_orders.get('active_orders', []):
                                        order_data = {
                                            "id": f"api_active_{account.index}_{api_order.get('id', 'unknown')}",
                                            "account_index": account.index,
                                            "market_index": api_order.get('market_id', 0),
                                            "order_type": api_order.get('order_type', 'unknown'),
                                            "side": api_order.get('side', 'unknown'),
                                            "amount": float(api_order.get('amount', 0)),
                                            "price": float(api_order.get('price', 0)),
                                            "status": api_order.get('status', 'unknown'),
                                            "created_at": api_order.get('created_at', 'unknown'),
                                            "filled_amount": float(api_order.get('filled_amount', 0)),
                                            "filled_price": float(api_order.get('filled_price', 0)) if api_order.get('filled_price') else None,
                                            "filled_at": api_order.get('filled_at'),
                                            "cancelled_at": api_order.get('cancelled_at'),
                                            "sdk_order_id": api_order.get('id'),
                                            "metadata": api_order,
                                            "source": "api_active"
                                        }
                                        api_orders.append(order_data)
                                    
                                    # 处理历史订单
                                    for api_order in account_orders.get('inactive_orders', []):
                                        order_data = {
                                            "id": f"api_inactive_{account.index}_{api_order.get('id', 'unknown')}",
                                            "account_index": account.index,
                                            "market_index": api_order.get('market_id', 0),
                                            "order_type": api_order.get('order_type', 'unknown'),
                                            "side": api_order.get('side', 'unknown'),
                                            "amount": float(api_order.get('amount', 0)),
                                            "price": float(api_order.get('price', 0)),
                                            "status": api_order.get('status', 'unknown'),
                                            "created_at": api_order.get('created_at', 'unknown'),
                                            "filled_amount": float(api_order.get('filled_amount', 0)),
                                            "filled_price": float(api_order.get('filled_price', 0)) if api_order.get('filled_price') else None,
                                            "filled_at": api_order.get('filled_at'),
                                            "cancelled_at": api_order.get('cancelled_at'),
                                            "sdk_order_id": api_order.get('id'),
                                            "metadata": api_order,
                                            "source": "api_inactive"
                                        }
                                        api_orders.append(order_data)
                                        
                                except Exception as account_error:
                                    logger.warning("获取账户订单失败", 
                                                  account_index=account.index, 
                                                  error=str(account_error))
                    
                    except Exception as api_error:
                        logger.warning("从API获取订单失败", error=str(api_error))
                
                # 合并所有订单
                all_orders = local_orders + api_orders
                
                # 按创建时间倒序排列
                try:
                    all_orders.sort(key=lambda x: x['created_at'], reverse=True)
                except:
                    # 如果排序失败，至少保证有数据返回
                    pass
                
                return {
                    "status": "success",
                    "data": all_orders,
                    "summary": {
                        "local_orders": len(local_orders),
                        "api_orders": len(api_orders),
                        "total_orders": len(all_orders)
                    }
                }
            except Exception as e:
                logger.error("获取订单信息失败", error=str(e))
                raise HTTPException(status_code=500, detail=str(e))
        
        @self.app.post("/api/positions/{position_id}/close")
        async def close_position(position_id: str):
            """手动平仓指定仓位"""
            try:
                strategy = getattr(self.trading_engine, 'strategy', None)
                if not strategy:
                    raise HTTPException(status_code=404, detail="交易策略未找到")
                
                # 查找指定仓位
                if not hasattr(strategy, 'active_positions') or position_id not in strategy.active_positions:
                    raise HTTPException(status_code=404, detail=f"仓位 {position_id} 未找到")
                
                hedge_position = strategy.active_positions[position_id]
                
                # 调用策略的平仓方法
                success = await strategy.close_hedge_position(
                    hedge_position, 
                    reason="manual_close", 
                    force_close=True
                )
                
                if success:
                    logger.info("手动平仓成功", position_id=position_id)
                    return {
                        "status": "success", 
                        "message": f"仓位 {position_id} 已成功平仓",
                        "position_id": position_id
                    }
                else:
                    logger.error("手动平仓失败", position_id=position_id)
                    return {
                        "status": "error", 
                        "message": f"仓位 {position_id} 平仓失败",
                        "position_id": position_id
                    }
                
            except HTTPException:
                raise
            except Exception as e:
                logger.error("手动平仓异常", position_id=position_id, error=str(e))
                raise HTTPException(status_code=500, detail=str(e))
        
        @self.app.post("/api/positions/close-all")
        async def close_all_positions():
            """同时平仓所有活跃仓位"""
            try:
                strategy = getattr(self.trading_engine, 'strategy', None)
                if not strategy:
                    raise HTTPException(status_code=404, detail="交易策略未找到")
                
                if not hasattr(strategy, 'active_positions'):
                    return {
                        "status": "info", 
                        "message": "没有活跃仓位需要平仓",
                        "closed_positions": []
                    }
                
                active_positions = list(strategy.active_positions.items())
                if not active_positions:
                    return {
                        "status": "info", 
                        "message": "没有活跃仓位需要平仓",
                        "closed_positions": []
                    }
                
                closed_positions = []
                failed_positions = []
                
                # 并行平仓所有仓位
                import asyncio
                
                async def close_single_position(pos_id, hedge_pos):
                    try:
                        success = await strategy.close_hedge_position(
                            hedge_pos, 
                            reason="manual_close_all", 
                            force_close=True
                        )
                        return pos_id, success, None
                    except Exception as e:
                        return pos_id, False, str(e)
                
                # 创建并发任务
                tasks = [
                    close_single_position(pos_id, hedge_pos) 
                    for pos_id, hedge_pos in active_positions
                ]
                
                # 执行并发平仓
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # 处理结果
                for result in results:
                    if isinstance(result, Exception):
                        failed_positions.append({"error": str(result)})
                    else:
                        pos_id, success, error = result
                        if success:
                            closed_positions.append(pos_id)
                        else:
                            failed_positions.append({"position_id": pos_id, "error": error})
                
                logger.info("批量平仓完成", 
                           closed_count=len(closed_positions),
                           failed_count=len(failed_positions))
                
                return {
                    "status": "completed",
                    "message": f"平仓完成: 成功 {len(closed_positions)} 个，失败 {len(failed_positions)} 个",
                    "closed_positions": closed_positions,
                    "failed_positions": failed_positions,
                    "total_processed": len(active_positions)
                }
                
            except Exception as e:
                logger.error("批量平仓异常", error=str(e))
                raise HTTPException(status_code=500, detail=str(e))
        
        @self.app.get("/api/market-data")
        async def get_market_data():
            """Get market data"""
            try:
                market_data = {}
                
                # 获取WebSocket管理器的市场数据
                ws_manager = getattr(self.trading_engine, 'ws_manager', None)
                if ws_manager and hasattr(ws_manager, 'latest_market_data'):
                    for market_index, data in ws_manager.latest_market_data.items():
                        market_data[market_index] = {
                            "market_index": market_index,
                            "price": float(data.price) if data.price else None,
                            "bid_price": float(data.bid_price) if data.bid_price else None,
                            "ask_price": float(data.ask_price) if data.ask_price else None,
                            "volume": float(data.volume) if data.volume else None,
                            "timestamp": data.timestamp.isoformat() if data.timestamp else None
                        }
                
                return {
                    "status": "success",
                    "data": market_data
                }
            except Exception as e:
                logger.error("获取市场数据失败", error=str(e))
                raise HTTPException(status_code=500, detail=str(e))
        
        @self.app.get("/api/config")
        async def get_config():
            """Get system configuration"""
            try:
                return {
                    "status": "success",
                    "data": {
                        "global": self.config_manager.get_global_config(),
                        "web": self.config_manager.get_web_config(),
                        "risk": self.config_manager.get_risk_config(),
                        "accounts_count": len(self.config_manager.get_accounts()),
                        "pairs_count": len(self.config_manager.get_trading_pairs())
                    }
                }
            except Exception as e:
                logger.error("获取配置信息失败", error=str(e))
                raise HTTPException(status_code=500, detail=str(e))
    
    def _get_dashboard_html(self) -> str:
        """Generate dashboard HTML"""
        return """
        <!DOCTYPE html>
        <html lang="zh-CN">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Lighter 对冲交易系统</title>
            <style>
                body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; margin: 0; padding: 20px; background: #f5f5f5; }
                .container { max-width: 1200px; margin: 0 auto; }
                .header { background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); margin-bottom: 20px; }
                .card { background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); margin-bottom: 20px; }
                .status { display: inline-block; padding: 4px 8px; border-radius: 4px; font-size: 12px; font-weight: bold; }
                .status.running { background: #d4edda; color: #155724; }
                .status.stopped { background: #f8d7da; color: #721c24; }
                .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px; }
                button { background: #007bff; color: white; border: none; padding: 10px 20px; border-radius: 4px; cursor: pointer; }
                button:hover { background: #0056b3; }
                .table { width: 100%; border-collapse: collapse; }
                .table th, .table td { padding: 8px; text-align: left; border-bottom: 1px solid #ddd; }
                .refresh-btn { float: right; background: #28a745; }
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>🚀 Lighter 对冲交易系统</h1>
                    <p>实时监控和管理您的对冲交易策略</p>
                    <button class="refresh-btn" onclick="location.reload()">🔄 刷新</button>
                </div>
                
                <div class="grid">
                    <div class="card">
                        <h3>📊 系统状态</h3>
                        <div id="system-status">加载中...</div>
                    </div>
                    
                    <div class="card">
                        <h3>💰 账户概览</h3>
                        <div id="accounts-overview">加载中...</div>
                    </div>
                    
                    <div class="card">
                        <h3>📈 市场数据</h3>
                        <div id="market-data">加载中...</div>
                    </div>
                    
                    <div class="card">
                        <h3>⚖️ 交易对</h3>
                        <div id="trading-pairs">加载中...</div>
                    </div>
                </div>
                
                <div class="card">
                    <h3>🎯 对冲仓位详情</h3>
                    <div id="hedge-positions">加载中...</div>
                </div>
                
                <div class="card">
                    <h3>📋 订单记录</h3>
                    <div id="orders-list">加载中...</div>
                </div>
                
                <div class="card">
                    <h3>🎛️ 控制面板</h3>
                    <button onclick="startEngine()">▶️ 启动引擎</button>
                    <button onclick="stopEngine()">⏹️ 停止引擎</button>
                    <button onclick="refreshData()">🔄 刷新数据</button>
                </div>
            </div>
            
            <script>
                async function fetchData(endpoint) {
                    try {
                        const response = await fetch('/api/' + endpoint);
                        const data = await response.json();
                        return data;
                    } catch (error) {
                        console.error('Error fetching ' + endpoint + ':', error);
                        return null;
                    }
                }
                
                async function updateSystemStatus() {
                    const data = await fetchData('status');
                    if (data && data.status === 'success') {
                        const system = data.data.system;
                        document.getElementById('system-status').innerHTML = `
                            <p><strong>状态:</strong> <span class="status ${system.is_running ? 'running' : 'stopped'}">${system.is_running ? '运行中' : '已停止'}</span></p>
                            <p><strong>活跃仓位:</strong> ${system.active_positions}</p>
                            <p><strong>账户总数:</strong> ${system.total_accounts}</p>
                            <p><strong>启用交易对:</strong> ${system.active_pairs}</p>
                        `;
                    }
                }
                
                async function updateAccounts() {
                    const data = await fetchData('accounts');
                    if (data && data.status === 'success') {
                        let html = '<table class="table"><tr><th>账户</th><th>余额</th><th>状态</th></tr>';
                        data.data.forEach(account => {
                            html += `<tr>
                                <td>${account.index}</td>
                                <td>${account.balance.toFixed(2)}</td>
                                <td><span class="status ${account.is_active ? 'running' : 'stopped'}">${account.is_active ? '活跃' : '非活跃'}</span></td>
                            </tr>`;
                        });
                        html += '</table>';
                        document.getElementById('accounts-overview').innerHTML = html;
                    }
                }
                
                async function updateHedgePositions() {
                    const data = await fetchData('positions');
                    if (data && data.status === 'success') {
                        if (data.data.length === 0) {
                            document.getElementById('hedge-positions').innerHTML = '<p>暂无对冲仓位</p>';
                        } else {
                            let html = '';
                            data.data.forEach(hedgePos => {
                                // 验证状态显示
                                let validationStatus = '';
                                if (hedgePos.backend_validated !== null) {
                                    if (hedgePos.backend_validated) {
                                        validationStatus = '<span style="color: green;">✅ 后端验证通过</span>';
                                    } else {
                                        validationStatus = '<span style="color: red;">❌ 后端验证失败</span>';
                                    }
                                }
                                
                                if (hedgePos.close_validated !== null) {
                                    if (hedgePos.close_validated) {
                                        validationStatus += ' <span style="color: green;">✅ 平仓验证通过</span>';
                                    } else {
                                        validationStatus += ' <span style="color: orange;">⚠️ 平仓待验证</span>';
                                    }
                                }
                                
                                html += `
                                    <div style="border: 1px solid #ddd; margin: 10px 0; padding: 15px; border-radius: 5px;">
                                        <h4>🎯 ${hedgePos.pair_id} - ${hedgePos.id.substring(0, 12)}...</h4>
                                        <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 10px; margin: 10px 0;">
                                            <div><strong>状态:</strong> <span class="status ${hedgePos.status === 'active' ? 'running' : 'stopped'}">${hedgePos.status}</span></div>
                                            <div><strong>策略:</strong> ${hedgePos.strategy}</div>
                                            <div><strong>总盈亏:</strong> <span style="color: ${hedgePos.total_pnl >= 0 ? 'green' : 'red'}">${hedgePos.total_pnl.toFixed(2)}</span></div>
                                            <div><strong>杠杆倍数:</strong> ${hedgePos.target_leverage}x</div>
                                            <div><strong>创建时间:</strong> ${new Date(hedgePos.created_at).toLocaleString()}</div>
                                        </div>
                                        
                                        ${validationStatus ? `<div style="margin: 10px 0; padding: 8px; background: #f8f9fa; border-radius: 4px;"><strong>🔍 验证状态:</strong> ${validationStatus}</div>` : ''}
                                        
                                        ${hedgePos.price_consistency_verified ? '<div style="color: green;">✅ 价格一致性验证通过</div>' : '<div style="color: orange;">⚠️ 价格一致性待验证</div>'}
                                        
                                        ${hedgePos.stop_loss_price ? `<div><strong>🛡️ 止损价格:</strong> ${hedgePos.stop_loss_price.toFixed(2)}</div>` : ''}
                                        ${hedgePos.take_profit_price ? `<div><strong>🎯 止盈价格:</strong> ${hedgePos.take_profit_price.toFixed(2)}</div>` : ''}
                                        
                                        <div style="margin-top: 10px;">
                                            <strong>📊 子仓位:</strong>
                                            <table class="table" style="margin-top: 5px;">
                                                <tr><th>账户</th><th>方向</th><th>数量</th><th>开仓价</th><th>当前价</th><th>盈亏</th></tr>
                                                ${hedgePos.positions.map(pos => `
                                                    <tr>
                                                        <td>${pos.account_index}</td>
                                                        <td><span style="color: ${pos.side === 'buy' ? 'green' : 'red'}">${pos.side.toUpperCase()}</span></td>
                                                        <td>${pos.amount.toFixed(2)}</td>
                                                        <td>${pos.entry_price ? pos.entry_price.toFixed(2) : '待更新'}</td>
                                                        <td>${pos.current_price ? pos.current_price.toFixed(2) : '待更新'}</td>
                                                        <td style="color: ${pos.unrealized_pnl >= 0 ? 'green' : 'red'}">${pos.unrealized_pnl.toFixed(2)}</td>
                                                    </tr>
                                                `).join('')}
                                            </table>
                                        </div>
                                    </div>
                                `;
                            });
                            document.getElementById('hedge-positions').innerHTML = html;
                        }
                    }
                }
                
                async function updateMarketData() {
                    const data = await fetchData('market-data');
                    if (data && data.status === 'success') {
                        const markets = Object.values(data.data);
                        if (markets.length === 0) {
                            document.getElementById('market-data').innerHTML = '<p>暂无市场数据</p>';
                        } else {
                            let html = '<table class="table"><tr><th>市场</th><th>价格</th><th>买价</th><th>卖价</th><th>更新时间</th></tr>';
                            markets.forEach(market => {
                                const updateTime = market.timestamp ? new Date(market.timestamp).toLocaleTimeString() : '未知';
                                html += `<tr>
                                    <td>市场 ${market.market_index}</td>
                                    <td><strong>${market.price ? market.price.toFixed(2) : '待更新'}</strong></td>
                                    <td>${market.bid_price ? market.bid_price.toFixed(2) : '待更新'}</td>
                                    <td>${market.ask_price ? market.ask_price.toFixed(2) : '待更新'}</td>
                                    <td>${updateTime}</td>
                                </tr>`;
                            });
                            html += '</table>';
                            document.getElementById('market-data').innerHTML = html;
                        }
                    }
                }
                
                async function updateOrdersList() {
                    const data = await fetchData('orders');
                    if (data && data.status === 'success') {
                        if (data.data.length === 0) {
                            document.getElementById('orders-list').innerHTML = '<p>暂无订单记录</p>';
                        } else {
                            // 按状态分组
                            const pendingOrders = data.data.filter(o => o.status === 'pending');
                            const filledOrders = data.data.filter(o => o.status === 'filled');
                            const failedOrders = data.data.filter(o => o.status === 'failed' || o.status === 'cancelled');
                            
                            let html = '';
                            
                            if (pendingOrders.length > 0) {
                                html += '<h4>⏳ 待处理订单</h4>';
                                html += '<table class="table"><tr><th>订单ID</th><th>账户</th><th>方向</th><th>数量</th><th>价格</th><th>创建时间</th></tr>';
                                pendingOrders.slice(0, 10).forEach(order => {
                                    html += `<tr>
                                        <td>${order.id.substring(0, 15)}...</td>
                                        <td>${order.account_index}</td>
                                        <td><span style="color: ${order.side === 'buy' ? 'green' : 'red'}">${order.side.toUpperCase()}</span></td>
                                        <td>${order.amount.toFixed(2)}</td>
                                        <td>${order.price.toFixed(2)}</td>
                                        <td>${new Date(order.created_at).toLocaleString()}</td>
                                    </tr>`;
                                });
                                html += '</table>';
                            }
                            
                            if (filledOrders.length > 0) {
                                html += '<h4>✅ 已成交订单 (最近10个)</h4>';
                                html += '<table class="table"><tr><th>订单ID</th><th>账户</th><th>方向</th><th>数量</th><th>成交价</th><th>成交时间</th></tr>';
                                filledOrders.slice(0, 10).forEach(order => {
                                    html += `<tr>
                                        <td>${order.id.substring(0, 15)}...</td>
                                        <td>${order.account_index}</td>
                                        <td><span style="color: ${order.side === 'buy' ? 'green' : 'red'}">${order.side.toUpperCase()}</span></td>
                                        <td>${order.filled_amount.toFixed(2)}</td>
                                        <td>${order.filled_price ? order.filled_price.toFixed(2) : order.price.toFixed(2)}</td>
                                        <td>${order.filled_at ? new Date(order.filled_at).toLocaleString() : '未知'}</td>
                                    </tr>`;
                                });
                                html += '</table>';
                            }
                            
                            if (failedOrders.length > 0) {
                                html += '<h4>❌ 失败/取消订单 (最近5个)</h4>';
                                html += '<table class="table"><tr><th>订单ID</th><th>账户</th><th>方向</th><th>状态</th><th>创建时间</th></tr>';
                                failedOrders.slice(0, 5).forEach(order => {
                                    html += `<tr>
                                        <td>${order.id.substring(0, 15)}...</td>
                                        <td>${order.account_index}</td>
                                        <td><span style="color: ${order.side === 'buy' ? 'green' : 'red'}">${order.side.toUpperCase()}</span></td>
                                        <td><span class="status stopped">${order.status}</span></td>
                                        <td>${new Date(order.created_at).toLocaleString()}</td>
                                    </tr>`;
                                });
                                html += '</table>';
                            }
                            
                            document.getElementById('orders-list').innerHTML = html;
                        }
                    }
                }
                
                async function updateTradingPairs() {
                    const data = await fetchData('trading-pairs');
                    if (data && data.status === 'success') {
                        let html = '<table class="table"><tr><th>交易对</th><th>状态</th><th>策略</th></tr>';
                        data.data.forEach(pair => {
                            html += `<tr>
                                <td>${pair.name}</td>
                                <td><span class="status ${pair.is_enabled ? 'running' : 'stopped'}">${pair.is_enabled ? '启用' : '禁用'}</span></td>
                                <td>${pair.hedge_strategy}</td>
                            </tr>`;
                        });
                        html += '</table>';
                        document.getElementById('trading-pairs').innerHTML = html;
                    }
                }
                
                async function startEngine() {
                    try {
                        const response = await fetch('/api/engine/start', { method: 'POST' });
                        const result = await response.json();
                        alert(result.message);
                        refreshData();
                    } catch (error) {
                        alert('启动失败: ' + error.message);
                    }
                }
                
                async function stopEngine() {
                    try {
                        const response = await fetch('/api/engine/stop', { method: 'POST' });
                        const result = await response.json();
                        alert(result.message);
                        refreshData();
                    } catch (error) {
                        alert('停止失败: ' + error.message);
                    }
                }
                
                async function refreshData() {
                    await updateSystemStatus();
                    await updateAccounts();
                    await updateMarketData();
                    await updateTradingPairs();
                    await updateHedgePositions();
                    await updateOrdersList();
                }
                
                // Initialize dashboard
                refreshData();
                
                // Auto refresh every 10 seconds
                setInterval(refreshData, 10000);
            </script>
        </body>
        </html>
        """
    
    async def initialize(self) -> None:
        """Initialize web server"""
        logger.info("Web服务器初始化完成")
    
    async def start(self) -> None:
        """Start web server"""
        try:
            web_config = self.config_manager.get_web_config()
            port = web_config.get('port', 3000)
            host = web_config.get('host', '0.0.0.0')
            
            config = uvicorn.Config(
                app=self.app,
                host=host,
                port=port,
                log_level="info"
            )
            
            self.server = uvicorn.Server(config)
            self.server_task = asyncio.create_task(self.server.serve())
            
            logger.info("Web服务器已启动", host=host, port=port)
            
        except Exception as e:
            logger.error("Web服务器启动失败", error=str(e))
            raise
    
    async def cleanup(self) -> None:
        """Cleanup web server"""
        try:
            if self.server:
                self.server.should_exit = True
            
            if self.server_task:
                self.server_task.cancel()
                try:
                    await self.server_task
                except asyncio.CancelledError:
                    pass
            
            logger.info("Web服务器已清理")
            
        except Exception as e:
            logger.error("Web服务器清理失败", error=str(e))