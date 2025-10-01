````

### 2. 安装依赖
```bash
pip install -r requirements.txt
````

````

### 4. 编辑配置文件
编辑 `config/config.yaml`，设置您的账户信息：

```yaml
accounts:
  - index: 你的账户索引 设置为0自动获取
    l1_address: "你的L1地址"
    private_key: "你的私钥"
    api_key_index: 你的API密钥索引
    is_active: true
````

````

## 🎯 运行系统

### 启动系统
```bash
python -m src.main start
````

### 启动选项

```bash
# 指定配置文件
python -m src.main -c custom-config.yaml start

# 设置日志级别
python -m src.main -l DEBUG start

# 指定日志目录
python -m src.main --log-dir ./custom-logs start
```

## 🌐 Web 界面

系统启动后，可通过以下地址访问：

- **Web 界面**: http://localhost:3000
- **API 文档**: http://localhost:3000/docs
- **系统状态**: http://localhost:3000/api/status

## 📊 API 接口

### 获取系统状态

```bash
curl http://localhost:3000/api/status
```

### 获取活跃仓位

```bash
curl http://localhost:3000/api/positions
```

### 获取账户信息

```bash
curl http://localhost:3000/api/accounts
```

### 控制交易引擎

```bash
# 启动引擎
curl -X POST http://localhost:3000/api/engine/start

# 停止引擎
curl -X POST http://localhost:3000/api/engine/stop
```

## ⚙️ 配置说明

### 主要配置项

```yaml
# 全局设置
global:
  api_url: "https://mainnet.zklighter.elliot.ai"
  monitoring_interval: 5 # 监控间隔(秒)
  enable_auto_trading: true

# 风险管理
risk_management:
  global_max_daily_loss: 5000
  emergency_stop_enabled: true

# 账户配置
accounts:
  - index: 165652
    l1_address: "0x..."
    private_key: "..."
    api_key_index: 111
    is_active: true
    risk_limits:
      max_daily_loss: 1000
      min_balance: 100

# 交易对配置
trading_pairs:
  - id: "btc-hedge-1"
    market_index: 0
    base_amount: 100000
    max_positions: 3
    cooldown_minutes: 10
    account_addresses:
      - "0x..."
      - "0x..."
```

## 🔍 开仓条件检查

系统会检查以下 8 个条件才会开仓：

1. ✅ **无正在开仓的仓位**
2. ✅ **无待处理订单**
3. ✅ **账户未锁定**
4. ✅ **未达到仓位限制**
5. ✅ **有足够可用账户**
6. ✅ **风控检查通过**
7. ✅ **不在冷却期**
8. ✅ **市场条件合适**
   - 市场开放
   - 价格数据有效
   - 波动率合理
   - 流动性充足
   - 价差合理

## 📝 日志系统

### 日志文件

- `logs/app.log` - 主应用日志
- `logs/error.log` - 错误日志
- `logs/trades.log` - 交易记录

### 日志级别

- DEBUG: 详细调试信息
- INFO: 一般信息
- WARNING: 警告信息
- ERROR: 错误信息

## 🛡️ 风险管理

### 多层级风险控制

- **全局级别**: 全局日损失限制、紧急停止
- **交易对级别**: 交易对日损失限制、仓位大小限制
- **账户级别**: 账户余额检查、日交易次数限制

### 风险事件处理

- 自动触发风险事件
- 可配置的风险处理动作
- 实时风险监控和告警

## 🔧 命令行工具

```bash
# 查看帮助
python -m src.main --help

# 初始化配置
python -m src.main init

# 验证配置
python -m src.main validate

# 查看系统状态
python -m src.main status

# 测试连接
python -m src.main test-connection

# 启动系统
python -m src.main start
```

## 🐛 故障排除

### 常见问题

1. **SDK 安装失败**

   ```bash
   pip install git+https://github.com/elliottech/lighter-python.git --force-reinstall
   ```

2. **连接失败**

   - 检查网络连接
   - 验证 API URL 配置
   - 确认私钥格式正确

3. **配置验证失败**
   - 检查 YAML 格式
   - 验证账户信息
   - 确认必填字段

### 调试模式

```bash
python -m src.main -l DEBUG start
```

## 📈 性能优化

- 使用异步 I/O 提高并发性能
- 配置合理的监控间隔
- 启用请求缓存和限流
- 优化 WebSocket 连接

## 🔒 安全注意事项

- 妥善保管私钥，不要提交到版本控制
- 使用环境变量存储敏感信息
- 定期更新依赖包
- 监控系统日志，及时发现异常

## 📞 支持与贡献

- 问题报告: 请创建 GitHub Issue
- 功能建议: 欢迎提交 Pull Request
- 文档改进: 帮助完善文档

## 📄 许可证

MIT License

---

**⚠️ 免责声明**: 本系统仅供学习和研究使用。使用本系统进行实际交易时，请务必充分了解相关风险，作者不承担任何交易损失责任。
