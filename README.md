# 晚霞推送 v6

> 0 bug 部署版 - 全部测试通过

## 部署 (飞牛 NAS)

```bash
# 1. 上传 sunset-bot-v6/ 到 NAS (建议 /vol1/1000/Docker/sunset-bot-v6/)
# 2. 进入目录
cd /vol1/1000/Docker/sunset-bot-v6
# 3. 一键安装
bash install.sh
# 4. 启动
./start.sh
# 5. 浏览器访问
#    http://NAS_IP:5000
#    首次访问要求设置密码
```

## 文件结构

```
sunset-bot-v6/
├── bot.py               # 核心：调度/评分/推送/升级 (560 行)
├── web.py               # Web：路由/SSE/鉴权 (470 行)
├── templates.html       # UI：单页 11 tab (1240 行)
├── migrate.py           # 迁移工具：export/import
├── install.sh           # 一键安装 (venv + 依赖)
├── start.sh / stop.sh   # 启停
├── run_tests.sh         # 跑全部测试
├── tests/               # 66+ 测试用例 (5 个文件)
└── README.md
```

## 测试

```bash
./run_tests.sh
# 21 + 8 + 9 + 14 + 14 = 66 用例全绿
```

## Web 路由 (16 个)

| GET | POST |
|---|---|
| `/` 主页 | `/api/auth/setup` 首次设密 |
| `/login` 登录页 | `/api/auth/login` 登录 |
| `/api/auth/status` 鉴权状态 | `/api/auth/logout` 登出 |
| `/api/sse` 实时事件流 | `/api/credentials` 保存凭证 |
| `/api/credentials` 读凭证 | `/api/locations` 加地点 |
| `/api/locations` 读地点 | `/api/locations/update` 改地点 |
| `/api/config` 读配置 | `/api/locations/delete` 删地点 |
| `/api/versions` 版本列表 | `/api/config` 写配置 |
| `/api/process/status` 进程状态 | `/api/upgrade/file` 上传升级 |
| `/api/diagnose` 一键诊断 | `/api/upgrade/rollback` 回滚 |
| `/api/history` 推送历史 | `/api/process/restart` 重启 |
| `/api/log/tail` 日志 tail | `/api/preview` 评分预览 |
| `/api/log/files` 日志文件 | `/api/manual/push` 手动推送 |

## 评分算法

总分 100 = 云量 40 + 湿度 25 + 风速 15 + 能见度 15 + 无降水 5
- 降水 > 0 硬过滤（下雨不推荐）
- 阈值可调（Web 阈值页）

## 升级/回滚

- Web 升级 tab → 选文件 → 自动备份（带微秒时间戳）→ 替换
- 回滚：选历史版本 → 一键回滚（自动备份当前）
- v6 修复 v4.1 备份互相覆盖 bug

## 数据迁移 (migrate.py)

```bash
python3 migrate.py export    # 打包所有
python3 migrate.py import x.tar.gz  # 导入
python3 migrate.py verify x.tar.gz  # 校验
python3 migrate.py list x.tar.gz    # 列出
```
