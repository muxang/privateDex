"""
Structured logging configuration for Lighter Hedge Trading System
"""

import structlog
import logging
import sys
from pathlib import Path
from typing import Any, Dict
import colorama
from colorama import Fore, Style
from datetime import datetime
import pytz

# Initialize colorama for colored console output
colorama.init()

# 北京时区
BEIJING_TZ = pytz.timezone('Asia/Shanghai')


def setup_logging(log_level: str = "INFO", log_dir: str = "logs") -> None:
    """Setup structured logging with both console and file outputs"""
    
    # Create log directory
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    
    # Configure standard library logging for file output only
    class BeijingFormatter(logging.Formatter):
        def formatTime(self, record, datefmt=None):
            from datetime import datetime
            dt = datetime.fromtimestamp(record.created, tz=BEIJING_TZ)
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        
        def format(self, record):
            # Remove logger name (module path)
            record.name = ""
            return super().format(record)
    
    # 设置自定义格式器
    formatter = BeijingFormatter("%(asctime)s [%(levelname)s] %(message)s")
    
    # 文件处理器
    file_handler_app = logging.FileHandler(f"{log_dir}/app.log", encoding='utf-8')
    file_handler_app.setFormatter(formatter)
    
    file_handler_error = logging.FileHandler(f"{log_dir}/error.log", encoding='utf-8')
    file_handler_error.setFormatter(formatter)
    file_handler_error.setLevel(logging.ERROR)
    
    # 配置根logger - 只用于文件输出
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, log_level.upper()))
    root_logger.handlers.clear()  # 清除默认处理器
    root_logger.addHandler(file_handler_app)
    root_logger.addHandler(file_handler_error)
    
    # Reduce WebSocket logging noise
    websocket_loggers = [
        'websockets.client',
        'websockets.server', 
        'websockets.protocol',
        'websockets',
        'lighter.ws_client',
        'lighter.client'
    ]
    
    for logger_name in websocket_loggers:
        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.WARNING)  # Only show warnings and errors
    
    # Reduce other noisy loggers
    noisy_loggers = [
        'aiohttp.access',
        'urllib3.connectionpool',
        'asyncio'
    ]
    
    for logger_name in noisy_loggers:
        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.WARNING)
    
    # Custom processor for Beijing time
    def add_beijing_timestamp(logger: Any, method_name: str, event_dict: Dict[str, Any]) -> Dict[str, Any]:
        """Add Beijing time timestamp"""
        beijing_time = datetime.now(BEIJING_TZ)
        event_dict["timestamp"] = beijing_time.strftime("%Y-%m-%d %H:%M:%S")
        return event_dict
    
    # Custom processor for colored console output  
    def add_color(logger: Any, method_name: str, event_dict: Dict[str, Any]) -> Dict[str, Any]:
        if not sys.stderr.isatty():
            return event_dict
            
        level = event_dict.get("level", "").upper()
        colors = {
            "DEBUG": Fore.CYAN,
            "INFO": Fore.GREEN,
            "WARNING": Fore.YELLOW,
            "ERROR": Fore.RED,
            "CRITICAL": Fore.RED + Style.BRIGHT,
        }
        
        color = colors.get(level, "")
        if color:
            event_dict["level"] = f"{color}{level}{Style.RESET_ALL}"
            
        return event_dict
    
    # Custom processor to remove logger name (file path)
    def remove_logger_name(logger: Any, method_name: str, event_dict: Dict[str, Any]) -> Dict[str, Any]:
        """Remove logger name (file path) from output"""
        event_dict.pop("logger", None)
        return event_dict
    
    # 简洁的控制台渲染器
    def console_renderer(logger, method_name, event_dict):
        """Clean console renderer without duplication"""
        timestamp = event_dict.pop("timestamp", "")
        level = event_dict.pop("level", "INFO").upper()
        event = event_dict.pop("event", "")
        
        # 构建参数字符串
        params = []
        for key, value in event_dict.items():
            if key not in ["logger", "level", "timestamp"]:
                params.append(f"{key}={value}")
        
        param_str = " ".join(params)
        
        if param_str:
            message = f"{timestamp} [{level}] {event} {param_str}"
        else:
            message = f"{timestamp} [{level}] {event}"
        
        # 直接输出到console，不通过logging模块
        print(message)
        return message
    
    # Configure structlog for console output only
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            add_beijing_timestamp,  # 添加北京时间
            remove_logger_name,     # 移除文件名信息
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            console_renderer,       # 使用控制台渲染器
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )


def get_logger(name: str = None) -> structlog.BoundLogger:
    """Get a structured logger instance"""
    return structlog.get_logger(name or __name__)


class TradeLogger:
    """Specialized logger for trading operations"""
    
    def __init__(self, log_dir: str = "logs"):
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.logger = get_logger("trading")
    
    def log_trade_opportunity(self, pair_id: str, opportunity_data: Dict[str, Any]) -> None:
        """Log a trading opportunity"""
        self.logger.info(
            "交易机会检测",
            pair_id=pair_id,
            **opportunity_data
        )
    
    def log_position_open(self, position_data: Dict[str, Any]) -> None:
        """Log position opening"""
        self.logger.info(
            "开仓成功",
            event_type="position_open",
            **position_data
        )
        
        # Also log to dedicated trades file
        with open(self.log_dir / "trades.log", "a", encoding='utf-8') as f:
            f.write(f"{position_data}\n")
    
    def log_position_close(self, position_data: Dict[str, Any]) -> None:
        """Log position closing"""
        self.logger.info(
            "平仓成功",
            event_type="position_close",
            **position_data
        )
        
        # Also log to dedicated trades file
        with open(self.log_dir / "trades.log", "a", encoding='utf-8') as f:
            f.write(f"{position_data}\n")
    
    def log_risk_event(self, event_data: Dict[str, Any]) -> None:
        """Log risk management events"""
        self.logger.warning(
            "风险事件触发",
            event_type="risk_event",
            **event_data
        )
    
    def log_error(self, error_msg: str, error_data: Dict[str, Any] = None) -> None:
        """Log trading errors"""
        self.logger.error(
            error_msg,
            event_type="trading_error",
            **(error_data or {})
        )


class PerformanceLogger:
    """Logger for performance metrics"""
    
    def __init__(self, log_dir: str = "logs"):
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.logger = get_logger("performance")
    
    def log_latency(self, operation: str, latency_ms: float, metadata: Dict[str, Any] = None) -> None:
        """Log operation latency"""
        self.logger.debug(
            "操作延迟",
            operation=operation,
            latency_ms=latency_ms,
            **(metadata or {})
        )
    
    def log_throughput(self, operation: str, count: int, duration_s: float) -> None:
        """Log operation throughput"""
        throughput = count / duration_s if duration_s > 0 else 0
        self.logger.info(
            "操作吞吐量",
            operation=operation,
            count=count,
            duration_s=duration_s,
            throughput_per_s=throughput
        )
    
    def log_system_metrics(self, metrics: Dict[str, Any]) -> None:
        """Log system performance metrics"""
        self.logger.info(
            "系统性能指标",
            **metrics
        )


# Global logger instances
trade_logger = TradeLogger()
performance_logger = PerformanceLogger()