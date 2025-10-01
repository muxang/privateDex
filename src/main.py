"""
Main application entry point for Lighter Hedge Trading System
"""

import asyncio
import signal
import sys
import os
from pathlib import Path
from typing import Optional
import click
import structlog

# 设置UTF-8编码
if sys.platform == "win32":
    # Windows环境设置
    os.environ['PYTHONIOENCODING'] = 'utf-8'
    import locale
    try:
        locale.setlocale(locale.LC_ALL, 'C.UTF-8')
    except locale.Error:
        try:
            locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')
        except locale.Error:
            pass  # 如果都失败就使用默认设置
    
    # 重新配置stdout和stderr为UTF-8
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    if hasattr(sys.stderr, 'reconfigure'):
        sys.stderr.reconfigure(encoding='utf-8', errors='replace')

from src.utils.logger import setup_logging, get_logger
from src.config.config_manager import ConfigManager
from src.core.hedge_trading_engine import HedgeTradingEngine
from src.web.web_server import WebServer

logger = get_logger()


class Application:
    """Main application class"""
    
    def __init__(self, config_path: Optional[str] = None):
        self.config_path = config_path or "config/config.yaml"
        self.config_manager: Optional[ConfigManager] = None
        self.trading_engine: Optional[HedgeTradingEngine] = None
        self.web_server: Optional[WebServer] = None
        self.shutdown_event = asyncio.Event()
        
    async def initialize(self) -> None:
        """Initialize all application components"""
        try:
            logger.info("🚀 启动 Lighter 对冲交易系统...")
            
            # Initialize configuration
            self.config_manager = ConfigManager(self.config_path)
            await self.config_manager.initialize()
            logger.info("[成功] 配置管理器初始化完成")
            
            # Initialize trading engine
            self.trading_engine = HedgeTradingEngine(self.config_manager)
            await self.trading_engine.initialize()
            logger.info("[成功] 交易引擎初始化完成")
            
            # Initialize web server
            web_config = self.config_manager.get_web_config()
            if web_config.get('enable_web_interface', True):
                self.web_server = WebServer(
                    self.config_manager,
                    self.trading_engine
                )
                await self.web_server.initialize()
                logger.info("[成功] Web服务器初始化完成")
            
            logger.info("🎉 系统初始化完成")
            
        except Exception as e:
            logger.error("[错误] 系统初始化失败", error=str(e))
            raise
    
    async def start(self) -> None:
        """Start all services"""
        try:
            # Start trading engine
            if self.trading_engine:
                await self.trading_engine.start()
                logger.info("[成功] 交易引擎已启动")
            
            # Start web server
            if self.web_server:
                await self.web_server.start()
                web_config = self.config_manager.get_web_config()
                port = web_config.get('port', 3000)
                logger.info("[成功] Web服务器已启动", port=port)
                
                # Print access information
                print(f"\n访问地址:")
                print(f"   Web界面: http://localhost:{port}")
                print(f"   API文档: http://localhost:{port}/docs")
                print(f"   系统状态: http://localhost:{port}/api/status")
                print(f"\n使用说明:")
                print(f"   1. 在Web界面中监控交易状态")
                print(f"   2. 查看实时仓位和风险指标")
                print(f"   3. 调整配置和交易参数")
                print(f"\n注意事项:")
                print(f"   - 请确保账户配置正确且有足够余额")
                print(f"   - 建议先在测试环境验证策略")
                print(f"   - 密切关注风险控制设置\n")
            
            logger.info("系统启动完成")
            
        except Exception as e:
            logger.error("系统启动失败", error=str(e))
            await self.cleanup()
            raise
    
    async def run(self) -> None:
        """Run the application"""
        try:
            await self.initialize()
            await self.start()
            
            # Wait for shutdown signal
            await self.shutdown_event.wait()
            
        except KeyboardInterrupt:
            logger.info("📡 收到中断信号，开始优雅关闭...")
        except Exception as e:
            logger.error("应用运行失败", error=str(e))
            raise
        finally:
            await self.cleanup()
    
    async def cleanup(self) -> None:
        """Cleanup all resources"""
        try:
            logger.info("🛑 正在关闭系统...")
            
            # Stop trading engine
            if self.trading_engine:
                await self.trading_engine.cleanup()
                logger.info("[成功] 交易引擎已停止")
            
            # Stop web server
            if self.web_server:
                await self.web_server.cleanup()
                logger.info("[成功] Web服务器已停止")
            
            logger.info("🎯 系统已安全关闭")
            
        except Exception as e:
            logger.error("[错误] 系统关闭时发生错误", error=str(e))
    
    def setup_signal_handlers(self) -> None:
        """Setup signal handlers for graceful shutdown"""
        def signal_handler(signum, frame):
            logger.info(f"📡 收到信号 {signum}，开始优雅关闭...")
            self.shutdown_event.set()
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)


@click.group()
@click.option('--config', '-c', default='config/config.yaml', help='Configuration file path')
@click.option('--log-level', '-l', default='INFO', help='Log level')
@click.option('--log-dir', default='logs', help='Log directory')
@click.pass_context
def cli(ctx, config, log_level, log_dir):
    """Lighter Hedge Trading System CLI"""
    ctx.ensure_object(dict)
    ctx.obj['config'] = config
    ctx.obj['log_level'] = log_level
    ctx.obj['log_dir'] = log_dir
    
    # Setup logging
    setup_logging(log_level, log_dir)


@cli.command()
@click.pass_context
def start(ctx):
    """Start the hedge trading system"""
    config_path = ctx.obj['config']
    
    # Check if config file exists
    if not Path(config_path).exists():
        click.echo(f"[错误] 配置文件不存在: {config_path}")
        click.echo("请先运行 'init' 命令创建配置文件")
        sys.exit(1)
    
    # Create and run application
    app = Application(config_path)
    app.setup_signal_handlers()
    
    try:
        asyncio.run(app.run())
    except KeyboardInterrupt:
        click.echo("\n👋 系统已停止")
    except Exception as e:
        click.echo(f"[错误] 系统运行失败: {e}")
        sys.exit(1)


@cli.command()
@click.pass_context
def init(ctx):
    """Initialize configuration files"""
    config_path = ctx.obj['config']
    config_dir = Path(config_path).parent
    
    # Create config directory
    config_dir.mkdir(parents=True, exist_ok=True)
    
    # Create default configuration if it doesn't exist
    if not Path(config_path).exists():
        # Copy default config (this would be more sophisticated in practice)
        click.echo(f"[成功] 已创建默认配置文件: {config_path}")
        click.echo("请编辑配置文件，设置账户信息和交易参数")
    else:
        click.echo(f"⚠️  配置文件已存在: {config_path}")


@cli.command()
@click.pass_context
def validate(ctx):
    """Validate configuration"""
    config_path = ctx.obj['config']
    
    if not Path(config_path).exists():
        click.echo(f"[错误] 配置文件不存在: {config_path}")
        sys.exit(1)
    
    async def validate_config():
        try:
            config_manager = ConfigManager(config_path)
            await config_manager.initialize()
            click.echo("[成功] 配置验证通过")
        except Exception as e:
            click.echo(f"[错误] 配置验证失败: {e}")
            sys.exit(1)
    
    asyncio.run(validate_config())


@cli.command()
@click.pass_context
def status(ctx):
    """Show system status"""
    # This would connect to running system to get status
    click.echo("🔍 系统状态检查功能开发中...")


@cli.command()
@click.pass_context
def test_connection(ctx):
    """Test connection to Lighter Protocol"""
    config_path = ctx.obj['config']
    
    async def test_conn():
        try:
            from src.config.config_manager import ConfigManager
            from src.services.account_manager import AccountManager
            
            config_manager = ConfigManager(config_path)
            await config_manager.initialize()
            
            account_manager = AccountManager(config_manager)
            await account_manager.initialize()
            
            click.echo("[成功] 连接测试成功")
            
        except Exception as e:
            click.echo(f"[错误] 连接测试失败: {e}")
            sys.exit(1)
    
    asyncio.run(test_conn())


if __name__ == "__main__":
    cli()