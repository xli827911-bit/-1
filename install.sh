#!/bin/bash
# 飞牛一键安装
set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "================================"
echo " 晚霞推送 v6 安装"
echo "================================"

# 检查 Python
if ! command -v python3 &> /dev/null; then
    echo "✗ 需 Python3，apt: sudo apt install python3 python3-venv python3-pip"
    exit 1
fi
PY=$(python3 -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
echo "✓ Python $PY"

# 创建 venv
if [ ! -d ".deps" ]; then
    python3 -m venv .deps
    echo "✓ 虚拟环境已创建"
fi
source .deps/bin/activate

# 装依赖
pip install --quiet --upgrade pip
pip install --quiet -r requirements.txt
echo "✓ 依赖已安装"

# 初始化数据
mkdir -p data data/logs _versions backups
python3 -c "import sys; sys.path.insert(0, '.'); import bot; bot.init_db(); print('✓ 数据库初始化完成')"

# 装 systemd 守护 (如果可用)
if command -v systemctl &> /dev/null; then
    SERVICE_FILE="/etc/systemd/system/sunset-bot.service"
    # 生成临时文件，再用 sudo 复制（避免直接写 /etc 权限问题）
    TMP_SERVICE=$(mktemp)
    cat > "$TMP_SERVICE" << EOF
[Unit]
Description=Sunset Bot v6 - 晚霞推送服务
After=network.target

[Service]
Type=oneshot
User=$USER
WorkingDirectory=$SCRIPT_DIR
ExecStart=/bin/bash $SCRIPT_DIR/start.sh
ExecStop=/bin/bash $SCRIPT_DIR/stop.sh
RemainAfterExit=yes
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF
    sudo cp "$TMP_SERVICE" "$SERVICE_FILE"
    rm -f "$TMP_SERVICE"
    sudo systemctl daemon-reload
    sudo systemctl enable sunset-bot
    echo "✓ systemd 开机自启已配置 (sunset-bot.service)"
fi

echo ""
echo "================================"
echo " 安装完成"
echo "================================"
echo ""
echo "启动: ./start.sh"
echo "停止: ./stop.sh"
echo "Web:  http://$(hostname -I | awk '{print $1}'):5000"
echo ""
