#!/bin/bash
# 启动 v6（语法校验 + 失败自动回滚）
set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

DEPLOY_DIR="$SCRIPT_DIR"

# 确定 Python 解释器
if [ -d "$DEPLOY_DIR/.deps" ]; then
    PY="$DEPLOY_DIR/.deps/bin/python3"
else
    PY="python3"
fi

# ==================== 停止旧进程（内联，不触发 stop.sh 的备份） ====================
for name in web bot; do
    PID_FILE="data/${name}.pid"
    if [ -f "$PID_FILE" ]; then
        PID=$(cat "$PID_FILE")
        if kill -0 "$PID" 2>/dev/null; then
            kill "$PID" 2>/dev/null
            for i in 1 2 3; do
                kill -0 "$PID" 2>/dev/null || break
                sleep 1
            done
            if kill -0 "$PID" 2>/dev/null; then
                kill -9 "$PID" 2>/dev/null
            fi
        fi
        rm -f "$PID_FILE"
    fi
done
# 兜底清理
for proc_pid in $(ps aux 2>/dev/null | grep "$DEPLOY_DIR" | grep -E "bot\.py|web\.py" | grep -v grep | awk '{print $2}'); do
    kill -9 "$proc_pid" 2>/dev/null || true
done
sleep 1

# ==================== 语法校验 ====================
echo "🔍 校验代码..."
SYNTAX_ERR=$($PY -c "
import sys, importlib
sys.path.insert(0, '.')
try:
    import bot
    import web
    print('OK')
except Exception as e:
    print(f'FAIL: {e}', file=sys.stderr)
    sys.exit(1)
" 2>&1) || {
    echo "❌ 代码校验失败："
    echo "$SYNTAX_ERR"
    echo ""

    # ==================== 自动回滚 ====================
    LATEST=$(ls -dt "$DEPLOY_DIR/_versions"/backup_* 2>/dev/null | head -1)
    if [ -n "$LATEST" ] && [ -d "$LATEST" ]; then
        VERSION_NAME=$(basename "$LATEST")
        echo "🔄 自动回滚到: $VERSION_NAME"

        # 调用 bot.py 的 restore_version()，与 Web UI 回滚逻辑一致
        ROLLBACK=$($PY -c "
import sys; sys.path.insert(0, '.')
import bot
ok, msg = bot.restore_version('$VERSION_NAME')
print(msg)
sys.exit(0 if ok else 1)
" 2>&1) || {
            echo "❌ 回滚失败: $ROLLBACK"
            echo "❌ 请手动检查！"
            exit 1
        }

        echo "✓ $ROLLBACK"

        # 再次校验回滚后的代码
        RECHECK=$($PY -c "
import sys; sys.path.insert(0, '.')
try:
    import bot; import web; print('OK')
except Exception as e:
    print(f'FAIL: {e}', file=sys.stderr); sys.exit(1)
" 2>&1) || {
            echo "❌ 回滚后代码仍有问题，请手动检查！"
            echo "$RECHECK"
            exit 1
        }

        echo "✓ 回滚后代码校验通过"
        echo "⚠ 服务未启动，请修复问题后重新执行: bash start.sh"
        exit 1
    else
        echo "❌ 无可用备份回滚，请手动修复！"
        exit 1
    fi
}

echo "✓ 代码校验通过"

# ==================== 启动服务 ====================
mkdir -p data/logs

# 后台启动 Web
nohup $PY web.py >> data/logs/web.out 2>&1 &
echo $! > data/web.pid
echo "✓ Web 已启动 (PID $(cat data/web.pid))"

# 启动 bot (调度)
nohup $PY bot.py >> data/logs/bot.out 2>&1 &
echo $! > data/bot.pid
echo "✓ Bot 已启动 (PID $(cat data/bot.pid))"
