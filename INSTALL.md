# 📅 每日数据自动同步安装指南

## 🎯 功能说明
系统会每天18:00自动同步当天的A股主板数据到MySQL数据库，完全无需人工干预。

## ✅ 同步功能测试结果
刚才的测试显示：
- ✅ 成功获取 **5,424条** 今日主板数据
- ✅ 数据库现有 **923,778条** 记录，覆盖 **5,454只** 股票
- ✅ 同步速度很快（约2秒完成）
- ✅ 自动去重，不会产生重复数据

## 🔧 Linux Cron 定时任务设置

### 1. 生成配置
```bash
cd /home/leonfyang/workspace/leontest
python main.py --install-cron
```

### 2. 复制显示的cron配置
系统会显示类似这样的配置：
```
0 18 * * 1-5 cd /home/leonfyang/workspace/leontest && /home/leonfyang/.pyenv/versions/3.12.3/bin/python /home/leonfyang/workspace/leontest/main.py --sync-today >> /home/leonfyang/workspace/leontest/daily_sync.log 2>&1
```

### 3. 安装到cron
```bash
# 编辑cron任务
crontab -e

# 将配置粘贴到文件末尾，保存退出

# 验证任务已添加
crontab -l
```

## 📊 监控和维护

### 查看同步日志
```bash
# 实时查看日志
tail -f daily_sync.log

# 查看最近50行日志
tail -50 daily_sync.log
```

### 手动测试同步
```bash
# 手动执行同步测试
python main.py --sync-today

# 查看数据库状态
python main.py --stats
```

### 查看最新数据
```bash
# 查看最新同步的数据
python main.py --query
```

## ⏰ 执行时间说明
- **执行时间**：每天 18:00
- **执行日期**：周一到周五（工作日）
- **自动跳过**：周末和节假日
- **智能检测**：如果当天不是交易日，会获取最新交易日数据

## 🔍 故障排除

### 检查cron服务
```bash
# 检查cron服务状态
systemctl status cron

# 查看cron执行日志
grep CRON /var/log/syslog | tail -10
```

### 检查权限
```bash
# 确保脚本可执行
ls -la main.py

# 确保日志文件可写
ls -la daily_sync.log
```

### 网络连接测试
```bash
# 测试网络连接
ping tushare.pro

# 测试API调用
python -c "import tushare as ts; ts.set_token('your_token'); print('API测试成功')"
```

## 📈 预期结果
设置完成后，系统将：
1. 每天18:00自动运行
2. 获取当天所有A股主板股票数据
3. 自动插入到MySQL数据库
4. 记录详细的执行日志
5. 自动处理重复数据

## 🎉 完成！
一旦设置完成，你的数据库将每天自动更新最新的A股主板数据，无需任何人工干预！
