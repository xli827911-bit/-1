#!/bin/bash
# 停止 v6（停止前自动备份代码，确保可回滚）
set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

DEPLOY_DIR="$SCRIPT_DIR"

# ==================== 自动备份（停止前快照） ====================
# 用标记文件防止 2 分钟内重复备份（start.sh 内部也会清理进程）
MARKER="$DEPLOY_DIR/data/.last_backup"
NEED_BACKUP=1

if [ -f "$MARKER" ]; then
    LAST_TS=$(cat "$MARKER")
    NOW_TS=$(date +%s)
    AGE=$((NOW_TS - LAST_TS))
    if [ "$AGE" -lt 120 ]; then
        NEED_BACKUP=0
    fi
fi

if [ "$NEED_BACKUP" -eq 1 ]; then
    if [ -d "$DEPLOY_DIR/.deps" ]; then
        PY="$DEPLOY_DIR/.deps/bin/python3"
    else
        PY="python3"
    fi

    # 调用 bot.py 的 backup_version()，与 Web UI 升级备份逻辑完全一致
    if [ -f "$DEPLOY_DIR/bot.py" ] && [ -f "$DEPLOY_DIR/web.py" ]; then
        BACKUP_RESULT=$($PY -c "
import sys; sys.path.insert(0, '.')
import bot
label = bot.backup_version(cleanup=True)
print(label)
" 2>/dev/null) || true

        if [ -n "$BACKUP_RESULT" ]; then
            # 统计备份文件数
            BACKUP_PATH="$DEPLOY_DIR/_versions/$BACKUP_RESULT"
            if [ -d "$BACKUP_PATH" ]; then
                FILE_COUNT=$(find "$BACKUP_PATH" -type f | wc -l)
                echo "📦 已备份当前代码 → $BACKUP_RESULT ($FILE_COUNT 个文件)"
            else
                echo "📦 已备份当前代码 → $BACKUP_RESULT"
            fi
            mkdir -p "$DEPLOY_DIR/data"
            date +%s > "$MARKER"
        fi
    fi
fi

# ==================== 停止进程 ====================
for name in web bot; do
    PID_FILE="data/${name}.pid"
    if [ -f "$PID_FILE" ]; then
        PID=$(cat "$PID_FILE")
        if kill -0 "$PID" 2>/dev/null; then
            kill "$PID" 2>/dev/null
            # 等待进程退出（最多 3 秒）
            for i in 1 2 3; do
                kill -0 "$PID" 2>/dev/null || break
                sleep 1
            done
            # 如果还没退出，强制杀
            if kill -0 "$PID" 2>/dev/null; then
                kill -9 "$PID" 2>/dev/null
                echo "⚠ $name (PID $PID) 强制终止"
            else
                echo "✓ $name 已停止 (PID $PID)"
            fi
        fi
        rm -f "$PID_FILE"
    fi
done

# 兜底：杀死本部署目录下所有相关进程（包括 root 启动的旧进程）
for proc_pid in $(ps aux 2>/dev/null | grep "$DEPLOY_DIR" | grep -E "bot\.py|web\.py" | grep -v grep | awk '{print $2}'); do
    kill -9 "$proc_pid" 2>/dev/null && echo "✓ 兜底清理 PID $proc_pid"
done
