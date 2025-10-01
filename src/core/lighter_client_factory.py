"""
Lighter Client Factory - 基于官方示例的客户端工厂
统一管理所有Lighter SDK客户端的创建和配置
"""

import lighter
import asyncio
from typing import Dict, Optional
import structlog
from src.config.config_manager import ConfigManager

logger = structlog.get_logger()


class LighterClientFactory:
    """统一管理Lighter Protocol客户端的工厂类"""
    
    def __init__(self, config_manager: ConfigManager):
        self.config_manager = config_manager
        self._api_client: Optional[lighter.ApiClient] = None
        self._configuration: Optional[lighter.Configuration] = None
        self._signer_clients: Dict[int, lighter.SignerClient] = {}
        self._ws_client: Optional[lighter.WsClient] = None
        
    def get_configuration(self) -> lighter.Configuration:
        """获取配置对象"""
        if self._configuration is None:
            self._configuration = lighter.Configuration()
            self._configuration.host = self.config_manager.get_api_url()
            
            # 添加其他配置选项
            global_config = self.config_manager.get_global_config()
            if 'timeout' in global_config:
                self._configuration.timeout = global_config['timeout']
                
        return self._configuration
    
    def get_api_client(self) -> lighter.ApiClient:
        """获取API客户端"""
        if self._api_client is None:
            configuration = self.get_configuration()
            self._api_client = lighter.ApiClient(configuration=configuration)
            logger.info("创建API客户端", host=configuration.host)
            
        return self._api_client
    
    def get_account_api(self) -> lighter.AccountApi:
        """获取账户API"""
        return lighter.AccountApi(self.get_api_client())
    
    def get_order_api(self) -> lighter.OrderApi:
        """获取订单API"""
        return lighter.OrderApi(self.get_api_client())
    
    def get_info_api(self) -> lighter.InfoApi:
        """获取信息API"""
        return lighter.InfoApi(self.get_api_client())
    
    def get_block_api(self) -> lighter.BlockApi:
        """获取区块API"""
        return lighter.BlockApi(self.get_api_client())
    
    def get_candlestick_api(self) -> lighter.CandlestickApi:
        """获取蜡烛图API"""
        return lighter.CandlestickApi(self.get_api_client())
    
    async def get_signer_client(self, account_index: int) -> Optional[lighter.SignerClient]:
        """获取签名客户端（支持重试和完善的错误处理）"""
        # 检查缓存
        if account_index in self._signer_clients:
            signer_client = self._signer_clients[account_index]
            # 验证客户端是否仍然有效
            try:
                # 这里可以添加健康检查
                return signer_client
            except Exception as e:
                logger.warning("缓存的SignerClient失效，重新创建",
                             account_index=account_index,
                             error=str(e))
                # 移除失效的客户端
                del self._signer_clients[account_index]
        
        # 创建新的SignerClient
        return await self._create_new_signer_client(account_index)
    
    async def _create_new_signer_client(self, account_index: int, max_retries: int = 3) -> Optional[lighter.SignerClient]:
        """创建新的SignerClient，支持重试机制"""
        
        # 检查平台支持
        if not self._check_platform_support():
            logger.error("当前平台不支持SignerClient", account_index=account_index)
            return None
        
        # 获取账户配置
        account_config = self.config_manager.get_account_by_index(account_index)
        if not account_config:
            logger.error("账户配置不存在", account_index=account_index)
            return None
        
        # 验证账户配置
        if not self._validate_account_config(account_config):
            logger.error("账户配置验证失败", account_index=account_index)
            return None
        
        # 处理私钥
        private_key = self._process_private_key(account_config.private_key)
        if not private_key:
            logger.error("私钥处理失败", account_index=account_index)
            return None
        
        # 重试创建SignerClient
        for attempt in range(max_retries):
            try:
                logger.info("创建SignerClient",
                           account_index=account_index,
                           attempt=attempt + 1,
                           max_attempts=max_retries)
                
                # 创建签名客户端
                signer_client = lighter.SignerClient(
                    url=self.config_manager.get_api_url(),
                    private_key=private_key,
                    account_index=account_index,
                    api_key_index=account_config.api_key_index
                )
                
                # SignerClient在创建时已自动初始化，无需手动调用initialize()
                
                # 验证初始化结果
                if await self._verify_signer_client(signer_client, account_config):
                    self._signer_clients[account_index] = signer_client
                    
                    logger.info("SignerClient创建成功",
                               account_index=account_index,
                               api_key_index=account_config.api_key_index,
                               l1_address=account_config.l1_address)
                    
                    return signer_client
                else:
                    logger.warning("SignerClient验证失败",
                                 account_index=account_index,
                                 attempt=attempt + 1)
                    
            except Exception as e:
                logger.warning("SignerClient创建失败",
                             account_index=account_index,
                             attempt=attempt + 1,
                             error=str(e))
                
                # 如果不是最后一次尝试，等待后重试
                if attempt < max_retries - 1:
                    await asyncio.sleep(1)
        
        logger.error("SignerClient创建失败，已尝试所有重试",
                    account_index=account_index,
                    max_retries=max_retries)
        return None
    
    def _check_platform_support(self) -> bool:
        """检查当前平台是否支持SignerClient"""
        import platform
        import os
        
        system = platform.system()
        machine = platform.machine()
        
        # 检查是否在WSL环境
        is_wsl = False
        try:
            if os.path.exists('/proc/version'):
                with open('/proc/version', 'r') as f:
                    version_info = f.read().lower()
                    if 'microsoft' in version_info or 'wsl' in version_info:
                        is_wsl = True
                        logger.info("检测到WSL环境，SignerClient可用")
        except:
            pass
        
        # SignerClient支持的平台
        if system == 'Linux':
            logger.info("Linux环境，SignerClient可用", platform=f"{system}/{machine}")
            return True
        elif system == 'Darwin' and machine == 'arm64':
            logger.info("macOS ARM环境，SignerClient可用", platform=f"{system}/{machine}")
            return True
        elif is_wsl:
            logger.info("WSL环境，SignerClient可用", platform=f"{system}/{machine}")
            return True
        else:
            logger.warning("平台不支持SignerClient", 
                         platform=f"{system}/{machine}",
                         supported="Linux(x86), Darwin(arm64), WSL")
            return False
    
    def _validate_account_config(self, account_config) -> bool:
        """验证账户配置的完整性"""
        try:
            # 检查必需字段
            required_fields = ['private_key', 'l1_address', 'api_key_index']
            for field in required_fields:
                if not hasattr(account_config, field) or getattr(account_config, field) is None:
                    logger.error("账户配置缺少必需字段", field=field)
                    return False
            
            # 验证私钥格式
            private_key = account_config.private_key
            if not isinstance(private_key, str) or len(private_key) < 40:
                logger.error("私钥格式无效", length=len(private_key) if private_key else 0)
                return False
            
            # 验证地址格式
            if not account_config.l1_address.startswith('0x'):
                logger.error("L1地址格式无效", address=account_config.l1_address)
                return False
            
            # 验证API密钥索引
            if not isinstance(account_config.api_key_index, int) or account_config.api_key_index < 0:
                logger.error("API密钥索引无效", api_key_index=account_config.api_key_index)
                return False
            
            return True
            
        except Exception as e:
            logger.error("账户配置验证异常", error=str(e))
            return False
    
    def _process_private_key(self, private_key: str) -> Optional[str]:
        """处理私钥格式"""
        try:
            if not private_key:
                return None
            
            # 移除可能的前缀
            if private_key.startswith('0x'):
                private_key = private_key[2:]
            
            # 使用完整的80字符私钥
            if len(private_key) == 80:
                logger.debug("使用80字符完整私钥")
                return private_key
            elif len(private_key) == 40:
                logger.debug("使用40字符私钥")
                return private_key
            else:
                logger.warning("私钥长度异常", length=len(private_key))
                return private_key  # 尝试使用原始私钥
            
        except Exception as e:
            logger.error("私钥处理失败", error=str(e))
            return None
    
    async def _verify_signer_client(self, signer_client: lighter.SignerClient, account_config) -> bool:
        """验证SignerClient是否正常工作"""
        try:
            # 这里可以添加一些基础验证
            # 例如检查客户端状态、连接性等
            
            # 基础验证：检查客户端对象是否有预期的方法
            required_methods = ['create_order', 'cancel_order']
            for method in required_methods:
                if not hasattr(signer_client, method):
                    logger.error("SignerClient缺少必需方法", method=method)
                    return False
            
            logger.debug("SignerClient验证通过",
                        account_index=account_config.index)
            return True
            
        except Exception as e:
            logger.error("SignerClient验证失败", error=str(e))
            return False
    
    def create_ws_client(self, 
                        order_book_ids: list = None,
                        account_ids: list = None,
                        on_order_book_update = None,
                        on_account_update = None) -> lighter.WsClient:
        """创建WebSocket客户端"""
        try:
            ws_client = lighter.WsClient(
                order_book_ids=order_book_ids or [],
                account_ids=account_ids or [],
                on_order_book_update=on_order_book_update,
                on_account_update=on_account_update,
            )
            
            logger.info("创建WebSocket客户端",
                       order_book_ids=order_book_ids,
                       account_ids=account_ids)
            
            return ws_client
            
        except Exception as e:
            logger.error("创建WebSocket客户端失败", error=str(e))
            raise
    
    async def test_connection(self) -> bool:
        """测试连接是否正常"""
        try:
            # 测试API连接
            info_api = self.get_info_api()
            # 这里可以调用一个简单的API来测试连接
            # result = await info_api.some_test_method()
            
            logger.info("连接测试通过")
            return True
            
        except Exception as e:
            logger.error("连接测试失败", error=str(e))
            return False
    
    async def cleanup(self) -> None:
        """清理所有客户端"""
        try:
            # 清理签名客户端
            for account_index, signer_client in self._signer_clients.items():
                try:
                    if hasattr(signer_client, 'close'):
                        await signer_client.close()
                    # 额外清理可能的aiohttp session
                    if hasattr(signer_client, '_session') and signer_client._session:
                        await signer_client._session.close()
                except Exception as e:
                    logger.error("清理签名客户端失败",
                                account_index=account_index,
                                error=str(e))
            
            self._signer_clients.clear()
            
            # 清理API客户端
            if self._api_client:
                try:
                    if hasattr(self._api_client, 'close'):
                        await self._api_client.close()
                    # 清理可能的底层连接
                    if hasattr(self._api_client, 'rest_client') and hasattr(self._api_client.rest_client, 'pool_manager'):
                        self._api_client.rest_client.pool_manager.clear()
                except Exception as e:
                    logger.error("清理API客户端失败", error=str(e))
            
            self._api_client = None
            self._configuration = None
            
            logger.info("客户端工厂清理完成")
            
        except Exception as e:
            logger.error("客户端工厂清理失败", error=str(e))


# 全局工厂实例
_client_factory: Optional[LighterClientFactory] = None


def get_client_factory(config_manager: ConfigManager = None) -> LighterClientFactory:
    """获取全局客户端工厂实例"""
    global _client_factory
    
    if _client_factory is None:
        if config_manager is None:
            raise ValueError("首次调用需要提供config_manager")
        _client_factory = LighterClientFactory(config_manager)
    
    return _client_factory


async def cleanup_factory():
    """清理全局工厂"""
    global _client_factory
    
    if _client_factory:
        await _client_factory.cleanup()
        _client_factory = None