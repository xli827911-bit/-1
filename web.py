#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
sunset-bot v6 - web.py
Web 路由：鉴权 / SSE / 文件上传 / 进程控制 / 诊断
"""
import os
import sys
import json
import time
import hmac
import gzip
import hashlib
import secrets
import sqlite3
import subprocess
import threading
import queue
import base64
import struct
import select as _select
import schedule
from pathlib import Path
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs
from datetime import datetime, timedelta

# 引入 bot 模块
sys.path.insert(0, str(Path(__file__).resolve().parent))
import bot

# ==================== 路径 ====================
BASE_DIR = Path(__file__).resolve().parent
TEMPLATES_FILE = BASE_DIR / "templates.html"
WEB_STARTUP_TIME = time.time()  # 进程启动时间戳，用于前端判断重启是否完成

# WebSocket 终端短期 token（绕过 HttpOnly cookie 限制）
_ws_tokens = {}

def _gen_ws_token():
    """生成一个 10 分钟有效的 WS 认证 token"""
    token = secrets.token_urlsafe(32)
    _ws_tokens[token] = time.time() + 600  # 10 分钟过期
    # 清理过期 token
    now = time.time()
    for k in [k for k, v in _ws_tokens.items() if v < now]:
        del _ws_tokens[k]
    return token

# ==================== 代理配置 ====================
def _apply_proxy_proxy(proxy_url: str):
    """应用 HTTPS 代理配置到当前进程"""
    import os
    if proxy_url:
        os.environ["https_proxy"] = proxy_url
        os.environ["http_proxy"] = proxy_url
        bot.log.info(f"[代理] 已设置代理: {proxy_url}")
    else:
        os.environ.pop("https_proxy", None)
        os.environ.pop("http_proxy", None)
        bot.log.info("[代理] 已清除代理配置")

def _load_proxy_from_config():
    """启动时从数据库加载代理配置"""
    try:
        proxy_url = bot.get_config("https_proxy", "")
        if proxy_url:
            _apply_proxy_proxy(proxy_url)
    except Exception as e:
        bot.log.debug(f"[代理] 加载配置失败: {e}")

# ==================== SSE 事件总线 ====================
class EventBus:
    """SSE 事件总线 - 所有客户端订阅"""
    def __init__(self):
        self.clients = []  # list of queue.Queue
        self.lock = threading.Lock()

    def publish(self, event_type, data):
        """发布事件到所有客户端"""
        msg = json.dumps({"type": event_type, "data": data, "time": time.time()}, ensure_ascii=False)
        with self.lock:
            dead = []
            for q in self.clients:
                try:
                    q.put_nowait(msg)
                except queue.Full:
                    dead.append(q)
            for q in dead:
                self.clients.remove(q)

    def subscribe(self):
        """订阅 - 返回新队列"""
        q = queue.Queue(maxsize=100)
        with self.lock:
            self.clients.append(q)
        return q

    def unsubscribe(self, q):
        with self.lock:
            if q in self.clients:
                self.clients.remove(q)


bus = EventBus()

# ==================== 登录限流 ====================
_login_attempts = {}  # {ip: [(timestamp, success), ...]}
_login_lock = threading.Lock()
LOGIN_MAX_FAILURES = 5       # 最大连续失败次数
LOGIN_LOCKOUT_SECONDS = 900  # 锁定 15 分钟

def _check_login_rate_limit(client_ip):
    """检查是否超过登录频率限制，返回 (allowed: bool, remaining_seconds: int)"""
    now = time.time()
    with _login_lock:
        attempts = _login_attempts.get(client_ip, [])
        # 清理超过锁定窗口的旧记录
        attempts = [(t, s) for t, s in attempts if now - t < LOGIN_LOCKOUT_SECONDS]
        # 统计最近连续失败次数
        recent_failures = 0
        for t, s in reversed(attempts):
            if s == 0:
                recent_failures += 1
            else:
                break
        if recent_failures >= LOGIN_MAX_FAILURES:
            # 计算剩余锁定时间
            oldest_failure = next((t for t, s in attempts if s == 0), now)
            remaining = int(LOGIN_LOCKOUT_SECONDS - (now - oldest_failure))
            _login_attempts[client_ip] = attempts
            return False, max(remaining, 1)
        _login_attempts[client_ip] = attempts
        return True, 0

def _record_login_attempt(client_ip, success):
    """记录登录尝试"""
    with _login_lock:
        attempts = _login_attempts.get(client_ip, [])
        attempts.append((time.time(), 1 if success else 0))
        _login_attempts[client_ip] = attempts
        if success:
            # 登录成功，清除记录
            _login_attempts.pop(client_ip, None)

# 手动预约推送任务跟踪（用于显示和取消）
_manual_scheduled_jobs = []  # [{"id": ..., "db_id": ..., "push_time": ..., "sunset_time": ..., "location": ..., "job": ...}]
_manual_job_counter = 0


def _restore_scheduled_pushes():
    """启动时从 DB 恢复预约任务"""
    global _manual_job_counter
    try:
        tasks = bot.get_scheduled_pushes()
        if not tasks:
            return
        locs = {l["id"]: l for l in bot.get_locations()}
        restored = 0
        for t in tasks:
            # 检查地点是否存在
            loc = locs.get(t["location_id"])
            if not loc:
                bot.delete_scheduled_push(t["id"])
                continue
            _manual_job_counter += 1
            job_id = _manual_job_counter
            db_id = t["id"]
            push_time_str = t["push_time"]
            sunset_time_str = t["sunset_time"]
            sunset_offset = t["sunset_offset"]
            
            def _scheduled_push(job_id=job_id, loc=loc, sunset_str=sunset_time_str, offset=sunset_offset, db_id=db_id, push_time_str=push_time_str):
                bot.log.info(f"=== 手动预约推送任务 [#{job_id}] [{loc['name']}] sunset={sunset_str} ===")
                bot.clear_weather_cache(loc["id"])
                weather, source, err = bot.fetch_weather_cached(loc["id"], loc["lng"], loc["lat"], force_refresh=True)
                if not weather:
                    bot.log.error(f"手动预约推送获取天气失败: {err}")
                else:
                    weather["sunset"] = sunset_str
                    weather["sunset_time"] = sunset_str
                    score_result = bot.score_sunset(weather)
                    text = bot.format_push_message(weather, score_result, loc["name"], sunset_str, source)
                    results = bot.push_message(text)
                    for ch, r in results.items():
                        bot.record_push(loc["id"], score_result["score"], ch, r["success"],
                                      f"[预约推送] sunset={sunset_str} score={score_result['score']}")
                    bot.audit("预约推送", loc["name"],
                             f"预约推送 sunset={sunset_str} score={score_result['score']}")
                    bot.log.info(f"预约推送完成: {loc['name']} score={score_result['score']}")
                for j in _manual_scheduled_jobs:
                    if j["id"] == job_id:
                        schedule.cancel_job(j["job"])
                        break
                _manual_scheduled_jobs[:] = [j for j in _manual_scheduled_jobs if j["id"] != job_id]
                if db_id:
                    bot.delete_scheduled_push(db_id)
                # 通知前端刷新任务列表
                bus.publish("scheduled_task_done", {"job_id": job_id, "location": loc["name"], "push_time": push_time_str})
                bot.log.info(f"[手动预约] 任务 #{job_id} 已执行完毕，自动清除")
            
            job = schedule.every().day.at(push_time_str).do(_scheduled_push)
            _manual_scheduled_jobs.append({
                "id": job_id,
                "db_id": db_id,
                "push_time": push_time_str,
                "sunset_time": sunset_time_str,
                "sunset_offset": sunset_offset,
                "location": loc["name"],
                "location_id": loc["id"],
                "job": job,
            })
            restored += 1
        if restored > 0:
            bot.log.info(f"[启动恢复] 已恢复 {restored} 个预约推送任务")
    except Exception as e:
        bot.log.warning(f"[启动恢复] 恢复预约任务失败: {e}")


# ==================== 鉴权 ====================
def hash_password(password, salt=None):
    """PBKDF2 哈希"""
    import hashlib
    if salt is None:
        salt = secrets.token_hex(16)
    h = hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), 100000)
    return f"{salt}${h.hex()}"


def verify_password(password, stored):
    """验证密码（使用 hmac.compare_digest 防止时序攻击）"""
    try:
        salt, hash_hex = stored.split("$", 1)
        h = hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), 100000)
        return hmac.compare_digest(h.hex(), hash_hex)
    except Exception:
        return False


def get_user():
    """获取当前用户（仅 1 个用户，id=1）"""
    conn = bot.get_db()
    row = conn.execute("SELECT id, password_hash FROM user WHERE id=1").fetchone()
    conn.close()
    return dict(row) if row else None


def create_user(password):
    """创建初始用户"""
    conn = bot.get_db()
    pw_hash = hash_password(password)
    conn.execute(
        "INSERT OR REPLACE INTO user (id, password_hash, created_at) VALUES (1, ?, ?)",
        (pw_hash, int(time.time()))
    )
    conn.commit()
    conn.close()


def create_session():
    """创建 session token"""
    token = secrets.token_urlsafe(32)
    conn = bot.get_db()
    expires = int(time.time()) + 30 * 86400  # 30 天
    conn.execute(
        "INSERT INTO sessions (token, created_at, expires_at) VALUES (?, ?, ?)",
        (token, int(time.time()), expires)
    )
    conn.commit()
    conn.close()
    return token


def verify_session(token):
    """验证 session"""
    if not token:
        return False
    conn = bot.get_db()
    row = conn.execute(
        "SELECT expires_at FROM sessions WHERE token=?", (token,)
    ).fetchone()
    conn.close()
    if not row:
        return False
    if row["expires_at"] < int(time.time()):
        return False
    return True


def cleanup_expired_sessions():
    """清理过期 session"""
    conn = bot.get_db()
    conn.execute("DELETE FROM sessions WHERE expires_at < ?", (int(time.time()),))
    conn.commit()
    conn.close()


def invalidate_all_sessions():
    """清空所有 session（启动时调用，防止部署包迁移后旧 session 自动登录）"""
    conn = bot.get_db()
    count = conn.execute("SELECT COUNT(*) FROM sessions").fetchone()[0]
    conn.execute("DELETE FROM sessions")
    conn.commit()
    conn.close()
    if count > 0:
        bot.log.info(f"[安全] 启动时清空 {count} 个旧 session，所有设备需重新登录")


# ==================== 工具函数 ====================
def _accepts_gzip(handler):
    """检查客户端是否接受 gzip 压缩"""
    ae = handler.headers.get("Accept-Encoding", "")
    return "gzip" in ae


def _send_compressed(handler, body_bytes, content_type):
    """发送响应体，大内容自动 gzip 压缩"""
    use_gzip = _accepts_gzip(handler) and len(body_bytes) > 1024
    handler.send_header("Content-Type", content_type)
    if use_gzip:
        compressed = gzip.compress(body_bytes, compresslevel=6)
        handler.send_header("Content-Encoding", "gzip")
        handler.send_header("Content-Length", str(len(compressed)))
        handler.end_headers()
        handler.wfile.write(compressed)
    else:
        handler.send_header("Content-Length", str(len(body_bytes)))
        handler.end_headers()
        handler.wfile.write(body_bytes)


def json_response(handler, data, status=200):
    handler.send_response(status)
    handler.send_header("Cache-Control", "no-store")
    body = json.dumps(data, ensure_ascii=False).encode("utf-8")
    _send_compressed(handler, body, "application/json; charset=utf-8")


def read_body(handler):
    length = int(handler.headers.get("Content-Length", 0))
    return handler.rfile.read(length) if length > 0 else b""


def get_session_token(handler):
    """从 cookie 取 session token"""
    cookie = handler.headers.get("Cookie", "")
    for part in cookie.split(";"):
        part = part.strip()
        if part.startswith("session="):
            return part[8:]
    return None


def is_logged_in(handler):
    """检查是否已登录"""
    return verify_session(get_session_token(handler))


def _extract_exif_datetime(image_bytes: bytes) -> str:
    """从图片 EXIF 数据中提取拍摄时间，返回 'YYYY-MM-DD HH:MM' 或 None"""
    try:
        from PIL import Image
        from PIL.ExifTags import TAGS
        import io
        img = Image.open(io.BytesIO(image_bytes))
        exif_data = img._getexif()
        if not exif_data:
            return None
        for tag_id, value in exif_data.items():
            tag = TAGS.get(tag_id, tag_id)
            if tag in ("DateTimeOriginal", "DateTimeDigitized", "DateTime"):
                # 格式: "2026:07:18 19:30:00"
                dt_str = str(value)
                # 转换为 "2026-07-18 19:30"
                parts = dt_str.split(" ")
                if len(parts) >= 2:
                    date_part = parts[0].replace(":", "-")
                    time_part = parts[1][:5]  # 取 HH:MM
                    return f"{date_part} {time_part}"
                return dt_str.replace(":", "-", 2)
    except Exception as e:
        bot.log.debug(f"[EXIF] 读取失败: {e}")
    return None


def parse_multipart(handler, content_type, body):
    """解析 multipart/form-data，简化版"""
    # 提取 boundary
    import re
    m = re.search(r"boundary=(.+)", content_type)
    if not m:
        return {}, []
    boundary = m.group(1).strip().encode()
    parts = body.split(b"--" + boundary)
    files = {}
    fields = {}
    file_index = 0  # 用于同名字段的去重
    for part in parts:
        if b"Content-Disposition" not in part:
            continue
        # 提取 name 和 filename
        cd_m = re.search(rb'name="([^"]+)"', part)
        fn_m = re.search(rb'filename="([^"]+)"', part)
        if not cd_m:
            continue
        name = cd_m.group(1).decode()
        # 提取内容
        header_end = part.find(b"\r\n\r\n")
        if header_end == -1:
            continue
        content = part[header_end + 4:]
        # 去尾
        if content.endswith(b"\r\n"):
            content = content[:-2]
        if fn_m:
            filename = fn_m.group(1).decode()
            # 用 filename 作为 key（避免多个同名字段互相覆盖）
            # 如果同名文件出现多次，用 index 区分
            key = filename if filename not in files else f"{filename}_{file_index}"
            files[key] = {"filename": filename, "content": content}
            file_index += 1
        else:
            fields[name] = content.decode("utf-8", errors="ignore")
    return fields, files


# ==================== HTTP Handler ====================
class Handler(BaseHTTPRequestHandler):

    def log_message(self, format, *args):
        # 走 bot 的 logger
        bot.log.info(f"[web] {self.address_string()} - {format % args}")

    def do_GET(self):
        path = urlparse(self.path).path
        # 公开路由
        if path == "/":
            self.serve_template()
        elif path == "/login":
            self.serve_login_page()
        elif path == "/manifest.json":
            self.serve_manifest()
        elif path == "/sw.js":
            self.serve_service_worker()
        elif path == "/api/auth/status":
            self.api_auth_status()
        elif path == "/api/sse":
            self.api_sse()
        # 受保护路由
        elif is_logged_in(self):
            self.route_get(path)
        else:
            json_response(self, {"error": "未登录"}, 401)

    def do_POST(self):
        path = urlparse(self.path).path
        if path == "/api/auth/login":
            self.api_login()
        elif path == "/api/auth/setup":
            self.api_setup()
            return
        elif not is_logged_in(self):
            json_response(self, {"error": "未登录"}, 401)
            return
        else:
            self.route_post(path)

    def route_get(self, path):
        routes = {
            "/api/credentials": self.api_get_credentials,
            "/api/locations": self.api_get_locations,
            "/api/config": self.api_get_config,
            "/api/versions": self.api_get_versions,
            "/api/process/status": self.api_process_status,
            "/api/diagnose": self.api_diagnose,
            "/api/health": self.api_health,
            "/api/history": self.api_get_history,
            "/api/log/tail": self.api_log_tail,
            "/api/log/files": self.api_log_files,
            "/api/schedule": self.api_get_schedule,
            "/api/sunset": self.api_get_sunset,
            "/api/sunset/manual/scheduled": self.api_sunset_scheduled_list,
            "/api/cache/weather": self.api_get_weather_cache,
            "/api/weather/providers": self.api_get_weather_providers,
            "/api/push_template": self.api_get_push_template,
            "/api/cloud_types": self.api_get_cloud_types,
            "/api/config/export": self.api_config_export,
            "/api/audit/logs": self.api_get_audit_logs,
            "/api/verify/data": self.api_verify_data,
            "/api/sunset/photos": self.api_get_sunset_photos,
            "/api/score/trend": self.api_score_trend,
            "/api/accuracy": self.api_accuracy,
            "/api/push/cleanup": self.api_push_cleanup,
            "/download/deploy-package": self.serve_deploy_package,
            "/api/terminal/download": self.api_terminal_download,
            "/api/filebrowser/list": self.api_filebrowser_list,
            "/api/update/status": self.api_update_status,
        }
        handler_fn = routes.get(path)
        if handler_fn:
            handler_fn()
        else:
            json_response(self, {"error": "Not found"}, 404)

    def route_post(self, path):
        routes = {
            "/api/auth/logout": self.api_logout,
            "/api/credentials": self.api_set_credential,
            "/api/credentials/delete": self.api_delete_credential,
            "/api/locations": self.api_add_location,
            "/api/locations/update": self.api_update_location,
            "/api/locations/delete": self.api_delete_location,
            "/api/config": self.api_set_config,
            "/api/upgrade/file": self.api_upgrade_file,
            "/api/upgrade/rollback": self.api_upgrade_rollback,
            "/api/versions/delete": self.api_delete_version,
            "/api/process/restart": self.api_process_restart,
            "/api/preview": self.api_preview,
            "/api/manual/push": self.api_manual_push,
            "/api/test/push": self.api_test_push,
            "/api/test/full": self.api_test_full,
            "/api/schedule": self.api_set_schedule,
            "/api/cache/refresh": self.api_refresh_cache,
            "/api/cache/clear": self.api_clear_cache,
            "/api/push_template": self.api_set_push_template,
            "/api/push/preview": self.api_push_preview,
            "/api/sunset/predict": self.api_sunset_predict,
            "/api/sunset/manual": self.api_sunset_manual,
            "/api/sunset/manual/schedule": self.api_sunset_manual_schedule,
            "/api/sunset/manual/scheduled/list": self.api_sunset_scheduled_list,
            "/api/sunset/manual/scheduled/cancel": self.api_sunset_scheduled_cancel,
            "/api/test/webhook": self.api_test_webhook,
            "/api/config/import": self.api_config_import,
            "/api/verify/compare": self.api_verify_compare,
            "/api/sunset/photo": self.api_sunset_photo_upload,
            "/api/sunset/photos/clear": self.api_clear_sunset_photos,
            "/api/config/score_offset": self.api_set_score_offset,
            "/api/terminal/upload": self.api_terminal_upload,
            "/api/update/check": self.api_update_check,
            "/api/update/apply": self.api_update_apply,
        }
        handler_fn = routes.get(path)
        if handler_fn:
            handler_fn()
        else:
            json_response(self, {"error": "Not found"}, 404)

    # ============ 页面 ============
    def serve_template(self):
        if not TEMPLATES_FILE.exists():
            self.send_response(500)
            self.end_headers()
            self.wfile.write(b"templates.html not found")
            return
        if not get_user():
            self.send_response(302)
            self.send_header("Location", "/login")
            self.end_headers()
            return
        if not is_logged_in(self):
            self.send_response(302)
            self.send_header("Location", "/login")
            self.end_headers()
            return
        self.send_response(200)
        self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
        self.send_header("Pragma", "no-cache")
        self.send_header("Expires", "0")
        html = TEMPLATES_FILE.read_text(encoding="utf-8")
        ws_token = _gen_ws_token()
        html = html.replace("/*WS_TOKEN_PLACEHOLDER*/", ws_token)
        _send_compressed(self, html.encode("utf-8"), "text/html; charset=utf-8")

    def serve_login_page(self):
        """登录/设置密码页 - 由 templates.html 同一文件渲染"""
        if not TEMPLATES_FILE.exists():
            self.send_response(500)
            self.end_headers()
            self.wfile.write(b"templates.html not found")
            return
        self.send_response(200)
        self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
        self.send_header("Pragma", "no-cache")
        self.send_header("Expires", "0")
        html = TEMPLATES_FILE.read_text(encoding="utf-8")
        ws_token = _gen_ws_token()
        html = html.replace("/*WS_TOKEN_PLACEHOLDER*/", ws_token)
        _send_compressed(self, html.encode("utf-8"), "text/html; charset=utf-8")

    def serve_manifest(self):
        """PWA manifest.json"""
        manifest_file = BASE_DIR / "manifest.json"
        if not manifest_file.exists():
            self.send_response(404)
            self.end_headers()
            return
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "public, max-age=86400")
        self.end_headers()
        self.wfile.write(manifest_file.read_bytes())

    def serve_service_worker(self):
        """PWA service worker"""
        sw_file = BASE_DIR / "sw.js"
        if not sw_file.exists():
            self.send_response(404)
            self.end_headers()
            return
        self.send_response(200)
        self.send_header("Content-Type", "application/javascript; charset=utf-8")
        self.send_header("Cache-Control", "public, max-age=3600")
        self.send_header("Service-Worker-Allowed", "/")
        self.end_headers()
        self.wfile.write(sw_file.read_bytes())

    def serve_deploy_package(self):
        """打包下载完整部署文件（用于部署到其他服务器）
        ?include_data=1 时同时打包 data/state.db 和密钥，新服务器开箱即用
        """
        import io
        import zipfile
        from urllib.parse import parse_qs
        qs = parse_qs(urlparse(self.path).query)
        include_data = qs.get("include_data", ["0"])[0] == "1"
        # 部署所需的核心文件
        deploy_files = [
            "bot.py", "providers.py", "scoring.py", "pusher.py",
            "web.py", "templates.html", "migrate.py",
            "requirements.txt", "install.sh", "start.sh", "stop.sh",
            "run_tests.sh", "README.md", "manifest.json", "sw.js",
            "sunset-bot-web.service",
        ]
        buf = io.BytesIO()
        included = []
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            for fname in deploy_files:
                fpath = BASE_DIR / fname
                if fpath.exists():
                    zf.write(fpath, f"sunset-bot/{fname}")
                    included.append(fname)
            # 打包数据库 + 密钥（含配置、凭证、地点；清除推送记录等历史数据）
            if include_data:
                import shutil
                import sqlite3
                import tempfile
                db_src = bot.DATA_DIR / "state.db"
                if db_src.exists():
                    # 复制到临时文件，清掉历史数据后再打包
                    tmp_db = Path(tempfile.gettempdir()) / f"sunset_export_{int(time.time())}.db"
                    shutil.copy2(db_src, tmp_db)
                    try:
                        conn = sqlite3.connect(str(tmp_db))
                        for table in ("push_history", "audit_log", "sessions",
                                      "weather_cache", "scheduled_push", "sunset_photos"):
                            try:
                                conn.execute(f"DELETE FROM [{table}]")
                            except Exception:
                                pass
                        conn.commit()
                        conn.execute("VACUUM")
                        conn.close()
                        zf.write(tmp_db, "sunset-bot/data/state.db")
                        included.append("data/state.db")
                    finally:
                        try:
                            tmp_db.unlink()
                        except Exception:
                            pass
                key_src = bot.DATA_DIR / ".secret_key"
                if key_src.exists():
                    zf.write(key_src, "sunset-bot/data/.secret_key")
                    included.append("data/.secret_key")
            # 附带部署说明
            readme = (
                "# 晚霞推送 v6 部署包\n\n"
                "## 快速部署\n"
                "```bash\n"
                "unzip sunset-bot-deploy.zip\n"
                "cd sunset-bot\n"
                "bash install.sh   # 安装依赖\n"
                "bash start.sh     # 启动服务\n"
                "```\n\n"
                + ("本包已包含 data/state.db 数据库（账号、凭证、地点、阈值等全部配置），\n"
                   "部署后使用原密码登录即可，无需重新配置。\n"
                   "（推送记录、审计日志等历史数据已清除）\n\n" if include_data else
                   "本包仅含代码文件，启动后需重新设置密码和各项配置。\n\n")
                + f"打包时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"包含文件: {', '.join(included)}\n"
            )
            zf.writestr("sunset-bot/DEPLOY.txt", readme)
        data = buf.getvalue()
        bot.audit("部署包下载", "-", f"下载部署包 include_data={include_data} ({len(included)} 个文件, {len(data)//1024} KB)")
        self.send_response(200)
        self.send_header("Content-Type", "application/zip")
        self.send_header("Content-Disposition", 'attachment; filename="sunset-bot-deploy.zip"')
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    # ============ 鉴权 API ============
    def api_auth_status(self):
        json_response(self, {
            "logged_in": is_logged_in(self),
            "has_user": get_user() is not None,
        })

    def api_setup(self):
        """首次设置密码"""
        if get_user() is not None:
            json_response(self, {"error": "用户已存在"}, 400)
            return
        body = json.loads(read_body(self) or b"{}")
        pw = body.get("password", "")
        if len(pw) < 6:
            json_response(self, {"error": "密码至少 6 位"}, 400)
            return
        create_user(pw)
        token = create_session()
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Set-Cookie", f"session={token}; Path=/; HttpOnly; Max-Age=2592000")
        self.end_headers()
        self.wfile.write(json.dumps({"ok": True}).encode())

    def api_login(self):
        # 登录限流检查
        client_ip = self.client_address[0]
        allowed, remaining = _check_login_rate_limit(client_ip)
        if not allowed:
            bot.log.warning(f"[安全] 登录限流: {client_ip} 被锁定 {remaining}s")
            json_response(self, {"error": f"登录尝试过多，请 {remaining} 秒后重试"}, 429)
            return
        body = json.loads(read_body(self) or b"{}")
        pw = body.get("password", "")
        user = get_user()
        if not user or not verify_password(pw, user["password_hash"]):
            _record_login_attempt(client_ip, False)
            bot.audit("登录失败", client_ip, f"密码错误 (IP: {client_ip})", "fail")
            json_response(self, {"error": "密码错误"}, 401)
            return
        _record_login_attempt(client_ip, True)
        token = create_session()
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Set-Cookie", f"session={token}; Path=/; HttpOnly; Max-Age=2592000")
        self.end_headers()
        self.wfile.write(json.dumps({"ok": True}).encode())

    def api_logout(self):
        token = get_session_token(self)
        if token:
            conn = bot.get_db()
            conn.execute("DELETE FROM sessions WHERE token=?", (token,))
            conn.commit()
            conn.close()
        self.send_response(200)
        self.send_header("Set-Cookie", "session=; Path=/; Max-Age=0")
        self.end_headers()
        self.wfile.write(b"{}")

    # ============ 凭证/地点/配置 ============
    def api_get_credentials(self):
        creds = bot.get_all_credentials()
        # token 脱敏
        masked = {}
        configured = {}
        for k, v in creds.items():
            configured[k] = bool(v and v.strip())
            if ("token" in k.lower() or "sendkey" in k.lower() or "key" in k.lower()) and v:
                masked[k] = v[:4] + "***" + v[-4:] if len(v) > 8 else "***"
            elif "webhook" in k.lower() and v:
                masked[k] = v[:30] + "***" if len(v) > 30 else v
            else:
                masked[k] = v
        json_response(self, {"values": masked, "configured": configured})

    def api_set_credential(self):
        body = json.loads(read_body(self) or b"{}")
        key = body.get("key", "")
        value = body.get("value", "")
        if not key:
            json_response(self, {"error": "key 必填"}, 400)
            return
        bot.set_credential(key, value)
        bot.audit("凭证更新", key, f"凭证更新: {key[:8]}...")
        bus.publish("credential_updated", {"key": key})
        json_response(self, {"ok": True, "key": key})

    def api_delete_credential(self):
        body = json.loads(read_body(self) or b"{}")
        key = body.get("key", "")
        if not key:
            json_response(self, {"error": "key 必填"}, 400)
            return
        bot.delete_credential(key)
        bot.audit("凭证删除", key, f"凭证删除: {key}")
        bus.publish("credential_deleted", {"key": key})
        json_response(self, {"ok": True, "key": key})

    def api_get_locations(self):
        json_response(self, bot.get_locations())

    def api_add_location(self):
        body = json.loads(read_body(self) or b"{}")
        name = body.get("name", "").strip()
        lng = body.get("lng")
        lat = body.get("lat")
        if not name or lng is None or lat is None:
            json_response(self, {"error": "name/lng/lat 必填"}, 400)
            return
        loc_id = bot.add_location(name, float(lng), float(lat))
        bot.audit("添加地点", name, f"添加地点: {name} ({lng},{lat})")
        bus.publish("location_added", {"id": loc_id, "name": name})
        json_response(self, {"ok": True, "id": loc_id})

    def api_update_location(self):
        body = json.loads(read_body(self) or b"{}")
        loc_id = body.get("id")
        if not loc_id:
            json_response(self, {"error": "id 必填"}, 400)
            return
        # 先获取原地名称
        locs = bot.get_locations()
        old_name = next((l["name"] for l in locs if l["id"] == loc_id), str(loc_id))
        ok = bot.update_location(
            loc_id,
            name=body.get("name"),
            lng=body.get("lng"),
            lat=body.get("lat"),
            enabled=body.get("enabled"),
        )
        bot.audit("编辑地点", body.get("name", old_name), f"编辑地点: {body.get('name', old_name)} (ID={loc_id})")
        bus.publish("location_updated", {"id": loc_id})
        json_response(self, {"ok": ok})

    def api_delete_location(self):
        body = json.loads(read_body(self) or b"{}")
        loc_id = body.get("id")
        if not loc_id:
            json_response(self, {"error": "id 必填"}, 400)
            return
        # 先获取地点名称
        locs = bot.get_locations()
        loc_name = next((l["name"] for l in locs if l["id"] == loc_id), str(loc_id))
        bot.delete_location(loc_id)
        bot.audit("删除地点", loc_name, f"删除地点: {loc_name} (ID={loc_id})")
        bus.publish("location_deleted", {"id": loc_id})
        json_response(self, {"ok": True})

    def api_get_config(self):
        json_response(self, bot.get_all_config())

    def api_set_config(self):
        body = json.loads(read_body(self) or b"{}")
        for k, v in body.items():
            bot.set_config(k, v)
        bot.audit("配置修改", str(list(body.keys())), str(body)[:200])
        # 如果修改了代理配置，立即应用
        if "https_proxy" in body:
            _apply_proxy_proxy(body["https_proxy"])
        bus.publish("config_updated", body)
        json_response(self, {"ok": True})

    # ============ 配置导入/导出 ============
    def api_config_export(self):
        """导出全部配置为 JSON"""
        data = bot.export_all_config()
        bot.audit("配置导出", None, "导出配置")
        json_response(self, data)

    def api_config_import(self):
        """从 JSON 导入配置"""
        body = json.loads(read_body(self) or b"{}")
        import_creds = body.get("import_credentials", False)
        results = bot.import_config(body, import_credentials=import_creds)
        bot.audit("配置导入", None, f"导入配置: {results}")
        bus.publish("config_updated", {})
        json_response(self, {"ok": True, "results": results})

    # ============ Webhook 连通性预检 ============
    def api_test_webhook(self):
        """测试单个 webhook 连通性"""
        body = json.loads(read_body(self) or b"{}")
        key = body.get("key", "")
        value = body.get("value", "")
        if not key:
            json_response(self, {"error": "key 必填"}, 400)
            return
        ok, msg = bot.test_webhook(key, value)
        bot.audit("Webhook测试", key, msg, "success" if ok else "fail")
        json_response(self, {"ok": ok, "message": msg})

    # ============ 审计日志 ============
    def api_get_audit_logs(self):
        """获取审计日志"""
        limit = int(urlparse(self.path).query.split("=")[1]) if "limit=" in urlparse(self.path).query else 100
        json_response(self, bot.get_audit_logs(limit))

    # ============ 调度时间 ============
    def api_get_schedule(self):
        times = bot.get_schedule_times()
        json_response(self, {"times": times})

    def api_set_schedule(self):
        body = json.loads(read_body(self) or b"{}")
        times = body.get("times", [])
        if not isinstance(times, list):
            json_response(self, {"error": "times 必须是数组"}, 400)
            return
        ok, msg = bot.set_schedule_times(times)
        if ok:
            bot.audit("调度时间", str(times), f"修改推送时间: {times}")
            bus.publish("schedule_updated", {"times": times})
            json_response(self, {"ok": True, "message": msg, "restart_required": True})
        else:
            json_response(self, {"error": msg}, 400)

    def api_get_sunset(self):
        """获取今天的日落调度信息"""
        info = bot.get_sunset_info()
        # 如果没有数据，实时计算一下
        if not info or not info.get("locations"):
            locs = bot.get_locations(enabled_only=True)
            if locs:
                sunset_offset = int(bot.get_config("sunset_offset_minutes", "50"))
                locations_info = []
                for loc in locs:
                    sunset_dt, source = bot.get_sunset_time_for_location(loc["lng"], loc["lat"])
                    push_time = sunset_dt - timedelta(minutes=sunset_offset)
                    locations_info.append({
                        "name": loc["name"],
                        "id": loc["id"],
                        "sunset_time": sunset_dt.strftime("%H:%M"),
                        "push_time": push_time.strftime("%H:%M"),
                        "sunset_offset": sunset_offset,
                        "source": source,
                    })
                info = {"locations": locations_info, "morning_time": "实时计算"}
        json_response(self, info)

    def api_sunset_predict(self):
        """基于缓存数据生成详细晚霞预测（评分+云型+逐小时趋势+推送预览）"""
        body = json.loads(read_body(self) or b"{}") if self.command == "POST" else {}
        loc_id = body.get("location_id")
        # 获取地点
        locs = bot.get_locations(enabled_only=True)
        if not locs:
            json_response(self, {"ok": False, "error": "没有启用的地点"}, 400)
            return
        loc = locs[0]
        if loc_id:
            loc = next((l for l in locs if l["id"] == loc_id), loc)
        # 获取缓存天气数据（不自动拉取 API，遵守 API 调用触发守则）
        weather, source, err = bot.fetch_weather_cached(loc["id"], loc["lng"], loc["lat"], force_refresh=False)
        if not weather:
            json_response(self, {"ok": False, "error": "暂无天气数据，请先点击「拉取最新天气」或使用「手动日落推送测试」"}, 400)
            return
        # 评分
        score_result = bot.score_sunset(weather)
        # 云型分类
        cloud = bot.classify_cloud_type(weather)
        # 日落时间
        sunset_time = weather.get("sunset_time", "") or weather.get("sunset", "")
        if not sunset_time:
            try:
                sunset_dt, _ = bot.get_sunset_time_for_location(loc["lng"], loc["lat"], weather=weather)
                sunset_time = sunset_dt.strftime("%H:%M") if sunset_dt else ""
            except Exception:
                pass
        # 用用户模板生成推送预览
        text = bot.format_push_message(weather, score_result, loc["name"], sunset_time, source)
        # 日落前后逐小时云量趋势（如果有 hourly 数据）
        hourly_trend = []
        # 评分维度明细
        breakdown = score_result.get("breakdown", {})
        json_response(self, {
            "ok": True,
            "location": loc["name"],
            "score": score_result["score"],
            "recommend": score_result["recommend"],
            "blocked": score_result.get("blocked", False),
            "breakdown": breakdown,
            "cloud_type": cloud,
            "sunset_time": sunset_time,
            "weather_summary": {
                "temperature": weather.get("temperature"),
                "apparent_temperature": weather.get("apparent_temperature"),
                "humidity": weather.get("humidity"),
                "cloudrate": weather.get("cloudrate"),
                "sunset_cloudrate": weather.get("sunset_cloudrate"),
                "cloud_high": weather.get("cloud_high"),
                "cloud_mid": weather.get("cloud_mid"),
                "cloud_low": weather.get("cloud_low"),
                "wind": weather.get("wind"),
                "visibility": weather.get("visibility"),
                "precipitation": weather.get("precipitation"),
                "skycon": weather.get("skycon"),
                "forecast_keypoint": weather.get("forecast_keypoint"),
            },
            "push_preview": text,
            "source": source,
        })

    def api_sunset_manual(self):
        """手动设置日落时间并执行推送测试"""
        body = json.loads(read_body(self) or b"{}")
        sunset_time_str = body.get("sunset_time", "")
        action = body.get("action", "preview")  # preview / push
        loc_id = body.get("location_id")
        custom_offset = body.get("sunset_offset")  # 可选：自定义偏移量
        
        # 验证日落时间格式
        if not sunset_time_str or ":" not in sunset_time_str:
            json_response(self, {"ok": False, "error": "请提供有效的日落时间（如 19:07）"}, 400)
            return
        
        try:
            parts = sunset_time_str.split(":")
            hour, minute = int(parts[0]), int(parts[1])
            if not (0 <= hour <= 23 and 0 <= minute <= 59):
                raise ValueError
        except (ValueError, IndexError):
            json_response(self, {"ok": False, "error": "日落时间格式无效，应为 HH:MM"}, 400)
            return
        
        # 获取地点（手动测试不限制启用状态）
        locs = bot.get_locations()
        if not locs:
            json_response(self, {"ok": False, "error": "没有地点，请先添加"}, 400)
            return
        loc = locs[0]
        if loc_id:
            loc = next((l for l in locs if l["id"] == loc_id), loc)
        
        # 计算推送时间（优先使用请求中的偏移量，否则用系统配置）
        if custom_offset is not None:
            try:
                sunset_offset = int(custom_offset)
                if not (10 <= sunset_offset <= 120):
                    raise ValueError
            except (ValueError, TypeError):
                json_response(self, {"ok": False, "error": "偏移量需为 10-120 之间的整数"}, 400)
                return
        else:
            sunset_offset = int(bot.get_config("sunset_offset_minutes", "50"))
        h, m = hour, minute
        push_minutes = h * 60 + m - sunset_offset
        if push_minutes < 0:
            push_minutes += 24 * 60  # 跨天
        push_h, push_m = divmod(push_minutes, 60)
        push_time_str = f"{push_h:02d}:{push_m:02d}"
        
        # 获取天气数据（强制刷新，支持交叉验证）
        bot.clear_weather_cache(loc["id"])
        if bot.get_config("cross_validation", False):
            weather, err = bot.fetch_weather_multi(loc["lng"], loc["lat"])
            source = weather.get("_source_label", "multi") if weather else "multi(error)"
        else:
            weather, source, err = bot.fetch_weather_cached(loc["id"], loc["lng"], loc["lat"], force_refresh=True)
        if not weather:
            json_response(self, {"ok": False, "error": f"获取天气失败: {err}"}, 400)
            return
        
        # 更新日落时间到 weather 数据
        weather["sunset"] = sunset_time_str
        weather["sunset_time"] = sunset_time_str
        
        # 评分
        score_result = bot.score_sunset(weather)
        
        # 生成推送文案
        text = bot.format_push_message(weather, score_result, loc["name"], sunset_time_str, source)
        
        result = {
            "ok": True,
            "location": loc["name"],
            "sunset_time": sunset_time_str,
            "push_time": push_time_str,
            "sunset_offset": sunset_offset,
            "score": score_result["score"],
            "recommend": score_result["recommend"],
            "blocked": score_result.get("blocked", False),
            "text": text,
            "source": source,
        }
        
        # 如果 action=push，立即执行推送
        if action == "push":
            push_results = bot.push_message(text)
            # 手动推送不记录到 push_history
            result["push_results"] = push_results
            result["pushed"] = True
            bot.audit("手动推送", loc["name"], 
                     f"手动推送 sunset={sunset_time_str} score={score_result['score']}")
        else:
            result["pushed"] = False
        
        json_response(self, result)

    def api_sunset_manual_schedule(self):
        """预约手动日落推送：计算推送时间，如果已过则立即执行，否则用 schedule 注册定时任务"""
        body = json.loads(read_body(self) or b"{}")
        sunset_time_str = body.get("sunset_time", "")
        loc_id = body.get("location_id")
        custom_offset = body.get("sunset_offset")
        
        # 验证日落时间格式
        if not sunset_time_str or ":" not in sunset_time_str:
            json_response(self, {"ok": False, "error": "请提供有效的日落时间（如 19:07）"}, 400)
            return
        
        try:
            parts = sunset_time_str.split(":")
            hour, minute = int(parts[0]), int(parts[1])
            if not (0 <= hour <= 23 and 0 <= minute <= 59):
                raise ValueError
        except (ValueError, IndexError):
            json_response(self, {"ok": False, "error": "日落时间格式无效，应为 HH:MM"}, 400)
            return
        
        # 获取地点（手动测试不限制启用状态）
        locs = bot.get_locations()
        if not locs:
            json_response(self, {"ok": False, "error": "没有地点，请先添加"}, 400)
            return
        loc = locs[0]
        if loc_id:
            loc = next((l for l in locs if l["id"] == loc_id), loc)
        
        # 计算推送时间
        if custom_offset is not None:
            try:
                sunset_offset = int(custom_offset)
                if not (10 <= sunset_offset <= 120):
                    raise ValueError
            except (ValueError, TypeError):
                json_response(self, {"ok": False, "error": "偏移量需为 10-120 之间的整数"}, 400)
                return
        else:
            sunset_offset = int(bot.get_config("sunset_offset_minutes", "50"))
        
        push_minutes = hour * 60 + minute - sunset_offset
        if push_minutes < 0:
            push_minutes += 24 * 60
        push_h, push_m = divmod(push_minutes, 60)
        push_time_str = f"{push_h:02d}:{push_m:02d}"
        
        # 判断推送时间是否已过
        now = datetime.now()
        push_time_today = now.replace(hour=push_h, minute=push_m, second=0, microsecond=0)
        executed_immediately = False
        push_results = None
        
        if push_time_today <= now:
            # 推送时间已过，立即执行
            executed_immediately = True
            bot.clear_weather_cache(loc["id"])
            weather, source, err = bot.fetch_weather_cached(loc["id"], loc["lng"], loc["lat"], force_refresh=True)
            if not weather:
                json_response(self, {"ok": False, "error": f"获取天气失败: {err}"}, 400)
                return
            weather["sunset"] = sunset_time_str
            weather["sunset_time"] = sunset_time_str
            score_result = bot.score_sunset(weather)
            text = bot.format_push_message(weather, score_result, loc["name"], sunset_time_str, source)
            push_results = bot.push_message(text)
            for ch, r in push_results.items():
                bot.record_push(loc["id"], score_result["score"], ch, r["success"],
                              f"[预约推送] sunset={sunset_time_str} score={score_result['score']}")
            bot.audit("预约推送", loc["name"],
                     f"预约推送 sunset={sunset_time_str} score={score_result['score']}")
        else:
            # 推送时间在未来，注册单次定时任务（执行后自动取消，不会每天重复）
            global _manual_job_counter
            _manual_job_counter += 1
            job_id = _manual_job_counter
            
            def _scheduled_push(job_id=job_id, loc=loc, sunset_str=sunset_time_str, offset=sunset_offset, db_id=None):
                bot.log.info(f"=== 手动预约推送任务 [#{job_id}] [{loc['name']}] sunset={sunset_str} ===")
                bot.clear_weather_cache(loc["id"])
                weather, source, err = bot.fetch_weather_cached(loc["id"], loc["lng"], loc["lat"], force_refresh=True)
                if not weather:
                    bot.log.error(f"手动预约推送获取天气失败: {err}")
                else:
                    weather["sunset"] = sunset_str
                    weather["sunset_time"] = sunset_str
                    score_result = bot.score_sunset(weather)
                    text = bot.format_push_message(weather, score_result, loc["name"], sunset_str, source)
                    results = bot.push_message(text)
                    for ch, r in results.items():
                        bot.record_push(loc["id"], score_result["score"], ch, r["success"],
                                      f"[预约推送] sunset={sunset_str} score={score_result['score']}")
                    bot.audit("预约推送", loc["name"],
                             f"预约推送 sunset={sunset_str} score={score_result['score']}")
                    bot.log.info(f"预约推送完成: {loc['name']} score={score_result['score']}")
                # 单次执行：执行后取消定时任务并从跟踪列表移除
                for j in _manual_scheduled_jobs:
                    if j["id"] == job_id:
                        schedule.cancel_job(j["job"])
                        break
                _manual_scheduled_jobs[:] = [j for j in _manual_scheduled_jobs if j["id"] != job_id]
                # 从 DB 删除
                if db_id:
                    bot.delete_scheduled_push(db_id)
                # 通知前端刷新任务列表
                bus.publish("scheduled_task_done", {"job_id": job_id, "location": loc["name"]})
                bot.log.info(f"[手动预约] 任务 #{job_id} 已执行完毕，自动清除")
            
            job = schedule.every().day.at(push_time_str).do(_scheduled_push)
            # 保存到 DB
            db_task_id = bot.save_scheduled_push(push_time_str, sunset_time_str, sunset_offset, loc["id"])
            # 更新闭包中的 db_id
            _scheduled_push.__defaults__ = (job_id, loc, sunset_time_str, sunset_offset, db_task_id)
            _manual_scheduled_jobs.append({
                "id": job_id,
                "db_id": db_task_id,
                "push_time": push_time_str,
                "sunset_time": sunset_time_str,
                "sunset_offset": sunset_offset,
                "location": loc["name"],
                "location_id": loc["id"],
                "job": job,
            })
            bot.log.info(f"[手动预约] 已注册单次推送任务 #{job_id}: {push_time_str} (日落{sunset_time_str} 提前{sunset_offset}分钟)")
        
        result = {
            "ok": True,
            "location": loc["name"],
            "sunset_time": sunset_time_str,
            "push_time": push_time_str,
            "sunset_offset": sunset_offset,
            "executed_immediately": executed_immediately,
        }
        if push_results:
            result["push_results"] = push_results
        
        json_response(self, result)

    def api_sunset_scheduled_list(self):
        """查询待执行的手动预约任务"""
        jobs = [{
            "id": j["id"],
            "push_time": j["push_time"],
            "sunset_time": j["sunset_time"],
            "sunset_offset": j["sunset_offset"],
            "location": j["location"],
        } for j in _manual_scheduled_jobs]
        json_response(self, {"jobs": jobs})

    def api_sunset_scheduled_cancel(self):
        """取消指定的预约任务"""
        body = json.loads(read_body(self) or b"{}")
        job_id = body.get("job_id")
        if job_id is None:
            json_response(self, {"ok": False, "error": "缺少 job_id"}, 400)
            return
        found = None
        for j in _manual_scheduled_jobs:
            if j["id"] == job_id:
                found = j
                break
        if not found:
            json_response(self, {"ok": False, "error": f"任务 #{job_id} 不存在"}, 404)
            return
        schedule.cancel_job(found["job"])
        _manual_scheduled_jobs[:] = [j for j in _manual_scheduled_jobs if j["id"] != job_id]
        # 从 DB 删除
        if found.get("db_id"):
            bot.delete_scheduled_push(found["db_id"])
        bot.log.info(f"[手动预约] 已取消任务 #{job_id}: {found['push_time']} ({found['location']})")
        bot.audit("取消预约", found["location"], f"取消预约任务 #{job_id}")
        json_response(self, {"ok": True, "message": f"已取消任务 #{job_id}"})

    def api_get_weather_cache(self):
        """获取天气缓存状态"""
        cache = bot.get_all_weather_cache()
        locs = {l["id"]: l for l in bot.get_locations()}
        result = []
        for loc_id, c in cache.items():
            loc = locs.get(loc_id, {})
            result.append({
                "location_id": loc_id,
                "location_name": loc.get("name", "未知"),
                "fetched_at": c["fetched_at"],
                "fetched_at_str": datetime.fromtimestamp(c["fetched_at"]).strftime("%Y-%m-%d %H:%M:%S"),
                "source": c["source"],
                "data": c["data"],
            })
        json_response(self, {"cache": result})

    def api_refresh_cache(self):
        """手动刷新天气缓存（调用 API）"""
        results = bot.refresh_all_weather_cache()
        bot.audit("刷新缓存", "天气数据", "手动刷新天气缓存")
        bus.publish("cache_refreshed", results)
        json_response(self, {"ok": True, "results": results})

    def api_clear_cache(self):
        """手动清除天气缓存"""
        bot.clear_weather_cache()
        bot.audit("清除缓存", "天气数据", "手动清除天气缓存")
        bus.publish("cache_cleared", {})
        json_response(self, {"ok": True, "message": "缓存已清除"})

    def api_get_weather_providers(self):
        """获取所有可用的天气 API 提供者"""
        providers = bot.get_available_providers()
        current = bot.get_config("weather_provider", "caiyun")
        json_response(self, {"providers": providers, "current": current})

    def api_get_push_template(self):
        """获取推送文案模板"""
        template = bot.get_config("push_template", bot.DEFAULT_PUSH_TEMPLATE)
        morning_template = bot.get_config("morning_push_template", bot.DEFAULT_MORNING_TEMPLATE)
        cloud_types = []
        for key, info in bot.SUNSET_CLOUD_TYPES.items():
            cloud_types.append({"key": key, "name": info["name"], "desc": info["desc"], "tip": info["tip"]})
        json_response(self, {
            "template": template,
            "morning_template": morning_template,
            "cloud_types": cloud_types,
            "variables": bot.PUSH_VARIABLES,
        })

    def api_set_push_template(self):
        """保存推送文案模板（支持傍晚和早间两套）"""
        body = json.loads(read_body(self) or b"{}")
        template = body.get("template", "")
        morning_template = body.get("morning_template", "")
        if template:
            bot.set_config("push_template", template)
        if morning_template:
            bot.set_config("morning_push_template", morning_template)
        if not template and not morning_template:
            json_response(self, {"error": "模板不能为空"}, 400)
            return
        bot.audit("推送模板", "模板配置", "更新推送文案模板")
        json_response(self, {"ok": True})

    def api_push_preview(self):
        """预览推送文案"""
        body = json.loads(read_body(self) or b"{}")
        template = body.get("template", bot.DEFAULT_PUSH_TEMPLATE)
        # 使用丰富的模拟数据生成预览
        mock_weather = {
            "cloudrate": 55, "humidity": 60, "wind": 3.2, "visibility": 15,
            "temperature": 28, "precipitation": 0,
            "cloud_high": 45, "cloud_mid": 30, "cloud_low": 15,
            "sunrise": "05:42", "sunset": "19:28",
            "temp_high": 32, "temp_low": 22,
            "precip_probability": 10, "skycon": "多云",
            "uv_desc": "强", "aqi": 45, "pressure": 1008,
            "dew_point": 18.5, "wind_direction": 125,
        }
        mock_score = {"score": 78, "recommend": True}
        cloud = bot.classify_cloud_type(mock_weather)
        rec_tag = "✅推荐拍摄" if mock_score["recommend"] else "⚠️条件一般"
        try:
            text = template.format(
                location="示例地点", score=78, recommend_tag=rec_tag,
                sunset_time="19:28", sunrise="05:42",
                temperature=28, apparent_temperature=31, temp_high=32, temp_low=22,
                humidity=60, cloudrate=55, sunset_cloudrate=48,
                wind=3.2, visibility=15,
                cloud_high=45, cloud_mid=30, cloud_low=15,
                precip_probability=10, skycon="多云",
                forecast_keypoint="多云转晴，午后有短时阵雨",
                uv_desc="强", aqi=45,
                comfort_desc="热", car_washing="较不适宜", dressing="很热", cold_risk="少发",
                cloud_type=cloud["name"], cloud_tip=cloud["tip"], data_source="天气API",
            )
        except Exception as e:
            text = f"模板格式错误: {e}"
        json_response(self, {"preview": text, "cloud_type": cloud})

    def api_get_cloud_types(self):
        """获取晚霞云层分类信息"""
        cloud_types = []
        for key, info in bot.SUNSET_CLOUD_TYPES.items():
            cloud_types.append({"key": key, "name": info["name"], "desc": info["desc"], "tip": info["tip"]})
        json_response(self, {"cloud_types": cloud_types})

    # ============ 升级/回滚 ============
    def api_upgrade_file(self):
        content_type = self.headers.get("Content-Type", "")
        body = read_body(self)
        fields, files = parse_multipart(self, content_type, body)
        if not files:
            json_response(self, {"error": "无文件"}, 400)
            return
        uploaded = {f["filename"]: f["content"] for f in files.values()}
        ok, msg = bot.apply_uploaded_files(uploaded)
        if not ok:
            json_response(self, {"error": msg}, 400)
            return
        # 通知前端升级完成（是否重启由前端决定）
        bot.audit("版本升级", str(list(uploaded.keys())), f"上传文件: {list(uploaded.keys())} 备份={msg}")
        bus.publish("upgrade_applied", {"files": list(uploaded.keys()), "backup": msg})
        json_response(self, {"ok": True, "backup": msg, "restart_required": True})

    def api_upgrade_rollback(self):
        body = json.loads(read_body(self) or b"{}")
        version = body.get("version", "")
        ok, msg = bot.restore_version(version)
        bot.audit("版本回滚", version, f"回滚到版本: {version} {'成功' if ok else '失败'}")
        bus.publish("upgrade_rolled_back", {"version": version, "success": ok})
        if ok:
            # 回滚成功后提示用户手动重启（不自动重启，因为旧版本 web.py 可能不兼容）
            json_response(self, {
                "ok": True, 
                "message": msg + "。请手动重启服务以使回滚生效：bash stop.sh && bash start.sh",
                "restart_required": True
            })
        else:
            json_response(self, {"ok": False, "message": msg})

    def api_get_versions(self):
        json_response(self, bot.list_versions())

    def api_delete_version(self):
        body = json.loads(read_body(self) or b"{}")
        version = body.get("version", "")
        if not version:
            json_response(self, {"error": "version 必填"}, 400)
            return
        ok, msg = bot.delete_version(version)
        bot.audit("删除版本", version, f"删除版本: {version} {'成功' if ok else '失败'}")
        bus.publish("version_deleted", {"version": version, "success": ok})
        json_response(self, {"ok": ok, "message": msg})

    # ============ 版本更新检查 ============
    def api_update_status(self):
        """获取当前版本 + 缓存的更新状态"""
        result = bot.check_github_update(force=False)
        result["github_repo"] = bot.get_config("github_repo", "")
        json_response(self, result)

    def api_update_check(self):
        """强制检查 GitHub 更新"""
        body = json.loads(read_body(self) or b"{}")
        # 允许前端临时设置 repo
        repo = body.get("github_repo", "")
        if repo:
            bot.set_config("github_repo", repo)
        result = bot.check_github_update(force=True)
        result["github_repo"] = bot.get_config("github_repo", "")
        bus.publish("update_checked", result)
        json_response(self, result)

    def api_update_apply(self):
        """从 GitHub 一键下载并应用更新"""
        files, err = bot.download_github_release()
        if err:
            json_response(self, {"ok": False, "error": err}, 400)
            return
        ok, msg = bot.apply_uploaded_files(files)
        if not ok:
            json_response(self, {"ok": False, "error": msg}, 400)
            return
        bot.audit("GitHub 一键更新", str(list(files.keys())), f"更新 {len(files)} 个文件，备份={msg}")
        bus.publish("upgrade_applied", {"files": list(files.keys()), "backup": msg})
        json_response(self, {"ok": True, "backup": msg, "files": list(files.keys()), "restart_required": True})

    # ============ 进程管理 ============
    def api_process_status(self):
        running = bot.is_running()
        pid = bot.get_pid()
        status = {"running": running, "pid": pid, "web_startup_time": WEB_STARTUP_TIME}
        if running:
            try:
                import psutil
                p = psutil.Process(pid)
                status["cpu"] = p.cpu_percent(interval=0.1)
                status["memory_mb"] = round(p.memory_info().rss / 1024 / 1024, 1)
                status["uptime_s"] = int(time.time() - p.create_time())
            except Exception:
                pass
        json_response(self, status)

    def api_process_restart(self):
        """重启 bot 进程 + web 进程自身（确保新代码/模板生效）"""
        body = {}
        try:
            body = json.loads(read_body(self) or b"{}")
        except Exception:
            pass
        cleanup = body.get("cleanup_push_history", False)
        if cleanup:
            conn = bot.get_db()
            before = conn.execute("SELECT COUNT(*) FROM push_history").fetchone()[0]
            conn.execute("DELETE FROM push_history WHERE message LIKE '[测试]%' OR message LIKE '[手动推送]%'")
            conn.commit()
            after = conn.execute("SELECT COUNT(*) FROM push_history").fetchone()[0]
            conn.close()
            bot.audit("推送历史清理", "-", f"删除 {before - after} 条非自动推送记录，剩余 {after} 条")
        bot.audit("重启服务", "bot+web", "手动重启服务")
        # 写一个标记文件，让 bot 检测到后自杀 + 由 web 拉起
        restart_flag = bot.DATA_DIR / ".restart_requested"
        restart_flag.write_text(str(int(time.time())))
        bot.log.info("收到重启请求")
        # 异步重启
        def do_restart():
            import time as t
            import sys
            t.sleep(1)
            if bot.is_running():
                pid = bot.get_pid()
                try:
                    os.kill(pid, 15)  # SIGTERM
                except Exception:
                    pass
            t.sleep(2)
            # 用 nohup 拉起 bot.py - 使用当前 Python 解释器（支持 venv）
            try:
                py_executable = sys.executable
                subprocess.Popen(
                    [py_executable, str(bot.BASE_DIR / "bot.py")],
                    cwd=str(bot.BASE_DIR),
                    stdout=open(bot.LOGS_DIR / "bot.out", "a"),
                    stderr=subprocess.STDOUT,
                    start_new_session=True,
                )
            except Exception as e:
                bot.log.error(f"重启 bot 失败: {e}")
            # 重启 web.py 自身（确保新模板/代码生效）
            t.sleep(1)
            try:
                py_executable = sys.executable
                web_script = str(bot.BASE_DIR / "web.py")
                bot.log.info("正在重启 web 进程...")
                os.execv(py_executable, [py_executable, web_script])
            except Exception as e:
                bot.log.error(f"重启 web 失败: {e}")
        threading.Thread(target=do_restart, daemon=True).start()
        bus.publish("process_restarted", {})
        json_response(self, {"ok": True, "message": "重启已触发"})

    # ============ 评分趋势 & 准确率分析 ============
    def api_score_trend(self):
        """近 7 日评分趋势 + 最新评分因子明细"""
        conn = bot.get_db()
        # 近 7 天每日最高评分
        rows = conn.execute("""
            SELECT DATE(pushed_at, 'unixepoch', 'localtime') as day,
                   MAX(score) as max_score,
                   AVG(score) as avg_score,
                   COUNT(*) as push_count
            FROM push_history
            WHERE pushed_at > strftime('%s', 'now', '-7 days')
            GROUP BY day ORDER BY day
        """).fetchall()
        trend = [{"day": r["day"], "max": r["max_score"], "avg": round(r["avg_score"] or 0, 1), "count": r["push_count"]} for r in rows]
        # 检查 detail 列是否存在（旧库可能没有此列）
        has_detail_col = any(c["name"] == "detail" for c in conn.execute("PRAGMA table_info(push_history)").fetchall())
        # 最新一条评分明细（用于雷达图）
        detail = None
        try:
            if has_detail_col:
                latest = conn.execute("""
                    SELECT h.score, h.detail, h.pushed_at, h.location_id, l.name as location_name
                    FROM push_history h LEFT JOIN locations l ON h.location_id = l.id
                    ORDER BY h.pushed_at DESC LIMIT 1
                """).fetchone()
            else:
                latest = conn.execute("""
                    SELECT h.score, h.pushed_at, h.location_id, l.name as location_name
                    FROM push_history h LEFT JOIN locations l ON h.location_id = l.id
                    ORDER BY h.pushed_at DESC LIMIT 1
                """).fetchone()
            if latest:
                detail = {"score": latest["score"], "location": latest["location_name"], "time": latest["pushed_at"]}
                # 尝试从 detail 字段解析因子
                has_factors = False
                if has_detail_col and latest["detail"]:
                    try:
                        d = json.loads(latest["detail"])
                        if d.get("cloudrate") is not None:
                            detail.update(d)
                            has_factors = True
                    except Exception:
                        pass
                # 没有因子明细时，从天气缓存中补全（雷达图需要）
                if not has_factors and latest["location_id"]:
                    try:
                        cache_row = conn.execute(
                            "SELECT data FROM weather_cache WHERE location_id = ?", (latest["location_id"],)
                        ).fetchone()
                        if cache_row and cache_row["data"]:
                            w = json.loads(cache_row["data"])
                            for key in ("cloudrate", "cloud_high", "cloud_mid", "humidity", "wind", "visibility"):
                                if key in w:
                                    detail[key] = w[key]
                    except Exception:
                        pass
        except Exception:
            pass
        conn.close()
        json_response(self, {"trend": trend, "latest": detail})

    def api_accuracy(self):
        """准确率分析：基于 AI 照片校准的预测偏差统计"""
        conn = bot.get_db()
        # 获取有照片校准记录的数据
        rows = conn.execute("""
            SELECT ph.score, ph.pushed_at, ph.location_id, l.name as location_name
            FROM push_history ph
            LEFT JOIN locations l ON ph.location_id = l.id
            WHERE ph.pushed_at > strftime('%s', 'now', '-30 days')
            ORDER BY ph.pushed_at DESC
        """).fetchall()
        total_pushes = len(rows)
        # 统计分数段分布
        score_dist = {"excellent": 0, "good": 0, "fair": 0, "poor": 0}
        for r in rows:
            s = r["score"] or 0
            if s >= 80: score_dist["excellent"] += 1
            elif s >= 65: score_dist["good"] += 1
            elif s >= 50: score_dist["fair"] += 1
            else: score_dist["poor"] += 1
        # 获取校准偏移量
        offsets = conn.execute("""
            SELECT AVG(ABS(CAST(value AS REAL))) as avg_offset,
                   COUNT(*) as cal_count
            FROM config WHERE key LIKE 'calibration_offset_%'
        """).fetchone()
        conn.close()
        avg_offset = round(offsets["avg_offset"] or 0, 1) if offsets else 0
        cal_count = offsets["cal_count"] if offsets else 0
        json_response(self, {
            "total_pushes": total_pushes,
            "score_distribution": score_dist,
            "avg_calibration_offset": avg_offset,
            "calibration_count": cal_count,
            "period": "30天",
        })

    def api_push_cleanup(self):
        """清理非自动推送记录（测试、手动推送）"""
        conn = bot.get_db()
        total_before = conn.execute("SELECT COUNT(*) FROM push_history").fetchone()[0]
        conn.execute("DELETE FROM push_history WHERE message LIKE '[测试]%' OR message LIKE '[手动推送]%'")
        conn.commit()
        total_after = conn.execute("SELECT COUNT(*) FROM push_history").fetchone()[0]
        conn.close()
        deleted = total_before - total_after
        bot.audit("推送历史清理", "-", f"删除 {deleted} 条非自动推送记录，剩余 {total_after} 条")
        json_response(self, {"ok": True, "deleted": deleted, "remaining": total_after})

    # ============ 诊断 ============
    def api_diagnose(self):
        """一键诊断：测当前天气API / 测推送通道 / 看系统"""
        results = {}

        # 1. 当前配置的天气 API 测试
        primary = bot.get_weather_provider()
        token = bot._get_provider_token(primary)
        if not token and primary.name not in ("openmeteo", "gfs", "ecmwf"):
            results["weather_api"] = {"status": "fail", "msg": f"{primary.display_name} 未配置 token"}
        else:
            try:
                weather, err = primary.fetch(116.4, 39.9, token, timeout=8)
                if weather:
                    results["weather_api"] = {"status": "ok", "msg": f"{primary.display_name} 可达 (云量={weather.get('cloudrate')}%)"}
                else:
                    results["weather_api"] = {"status": "fail", "msg": f"{primary.display_name}: {err}"}
            except Exception as e:
                results["weather_api"] = {"status": "fail", "msg": f"{primary.display_name}: {e}"}

        # 2. 所有推送通道连通性检测（仅检查配置，不实际推送）
        push_channels = {
            "wechat": "wechat_webhook",
            "dingtalk": "dingtalk_webhook",
            "feishu": "feishu_webhook",
            "serverchan": "serverchan_key",
            "pushplus": "pushplus_token",
        }
        for ch_name, cred_key in push_channels.items():
            configured = bool(bot.get_credential(cred_key, ""))
            results[ch_name] = {
                "status": "ok" if configured else "skip",
                "msg": "已配置" if configured else "未配置",
            }

        # 3. 系统信息
        try:
            import psutil
            results["system"] = {
                "status": "ok",
                "msg": f"CPU {psutil.cpu_percent()}% / 内存 {psutil.virtual_memory().percent}%",
            }
        except Exception:
            results["system"] = {"status": "skip", "msg": "psutil 不可用"}

        # 4. 数据目录
        data_ok = bot.DATA_DIR.exists() and bot.DB_PATH.exists()
        results["data"] = {
            "status": "ok" if data_ok else "fail",
            "msg": f"DB {bot.DB_PATH.stat().st_size if bot.DB_PATH.exists() else 0} bytes",
        }

        bus.publish("diagnose_done", results)
        json_response(self, results)

    def api_health(self):
        """服务健康检查：检查整个项目服务是否正常运行"""
        import subprocess
        checks = {}
        all_ok = True
        
        # 1. bot.py 进程状态
        bot_running = bot.is_running()
        checks["bot_process"] = {
            "name": "bot.py 进程",
            "status": "ok" if bot_running else "fail",
            "msg": f"运行中 (PID {bot.get_pid()})" if bot_running else "未运行",
        }
        if not bot_running:
            all_ok = False
        
        # 2. web.py 进程状态 (当前进程肯定在运行)
        checks["web_process"] = {
            "name": "web.py 进程",
            "status": "ok",
            "msg": f"运行中 (PID {os.getpid()})",
        }
        
        # 3. 端口监听状态
        try:
            import socket
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(2)
            result = sock.connect_ex(('127.0.0.1', 5000))
            sock.close()
            port_ok = result == 0
            checks["port"] = {
                "name": "端口 5000",
                "status": "ok" if port_ok else "fail",
                "msg": "正常监听" if port_ok else "未监听",
            }
            if not port_ok:
                all_ok = False
        except Exception as e:
            checks["port"] = {"name": "端口 5000", "status": "fail", "msg": str(e)}
            all_ok = False
        
        # 4. 磁盘空间
        try:
            import shutil
            total, used, free = shutil.disk_usage(str(bot.BASE_DIR))
            usage_pct = (used / total) * 100
            disk_ok = usage_pct < 90
            checks["disk"] = {
                "name": "磁盘空间",
                "status": "ok" if disk_ok else "warning",
                "msg": f"已用 {usage_pct:.1f}% ({free // (1024**3)}GB 可用)",
            }
            if not disk_ok:
                all_ok = False
        except Exception as e:
            checks["disk"] = {"name": "磁盘空间", "status": "skip", "msg": str(e)}
        
        # 5. 最近日志错误
        try:
            log_file = bot.LOGS_DIR / "bot.out"
            if log_file.exists():
                with open(log_file, 'r', encoding='utf-8', errors='ignore') as f:
                    lines = f.readlines()
                    recent_lines = lines[-50:] if len(lines) > 50 else lines
                    error_count = sum(1 for line in recent_lines if 'ERROR' in line or 'Exception' in line)
                checks["logs"] = {
                    "name": "最近日志",
                    "status": "ok" if error_count == 0 else "warning",
                    "msg": f"最近 50 行有 {error_count} 个错误" if error_count > 0 else "无异常",
                }
            else:
                checks["logs"] = {"name": "最近日志", "status": "skip", "msg": "日志文件不存在"}
        except Exception as e:
            checks["logs"] = {"name": "最近日志", "status": "skip", "msg": str(e)}
        
        # 6. 数据库状态
        db_ok = bot.DB_PATH.exists()
        checks["database"] = {
            "name": "数据库",
            "status": "ok" if db_ok else "fail",
            "msg": f"正常 ({bot.DB_PATH.stat().st_size} bytes)" if db_ok else "不存在",
        }
        if not db_ok:
            all_ok = False
        
        json_response(self, {"overall": "ok" if all_ok else "warning", "checks": checks})

    # ============ 历史/日志 ============
    def api_get_history(self):
        limit = int(urlparse(self.path).query.split("=")[1]) if "limit=" in urlparse(self.path).query else 50
        json_response(self, bot.get_push_history(limit))

    def api_log_tail(self):
        """tail 最新日志"""
        from datetime import datetime as _dt
        # 优先读当天日志 bot_YYYYMMDD.log，回退到 bot.log
        today_file = bot.LOGS_DIR / f"bot_{_dt.now().strftime('%Y%m%d')}.log"
        if today_file.exists():
            log_file = today_file
        elif (bot.LOGS_DIR / "bot.log").exists():
            log_file = bot.LOGS_DIR / "bot.log"
        else:
            fallback = sorted(bot.LOGS_DIR.glob("bot_*.log"), reverse=True)
            if not fallback:
                json_response(self, {"lines": [], "file": None})
                return
            log_file = fallback[0]
        try:
            with open(log_file, "r", encoding="utf-8", errors="ignore") as f:
                lines = f.readlines()[-200:]
            json_response(self, {"lines": lines, "file": log_file.name})
        except Exception as e:
            json_response(self, {"error": str(e)}, 500)

    def api_log_files(self):
        all_logs = list(bot.LOGS_DIR.glob("bot.log*")) + list(bot.LOGS_DIR.glob("bot_*.log"))
        # 去重并按修改时间倒序
        seen = set()
        unique = []
        for f in sorted(all_logs, key=lambda x: x.stat().st_mtime, reverse=True):
            if f.name not in seen:
                seen.add(f.name)
                unique.append(f)
        json_response(self, [{"name": f.name, "size_kb": round(f.stat().st_size / 1024, 1)} for f in unique])

    # ============ 预览/手动推送 ============
    def api_preview(self):
        """预览评分（不实际推送，使用缓存数据）"""
        body = json.loads(read_body(self) or b"{}")
        loc_id = body.get("location_id")
        if not loc_id:
            json_response(self, {"error": "location_id 必填"}, 400)
            return
        locs = {l["id"]: l for l in bot.get_locations()}
        if loc_id not in locs:
            json_response(self, {"error": "地点不存在"}, 404)
            return
        loc = locs[loc_id]
        # 使用缓存数据（不强制刷新 API）
        weather, source, err = bot.fetch_weather_cached(loc_id, loc["lng"], loc["lat"], force_refresh=False)
        if err and not weather:
            json_response(self, {"error": err}, 500)
            return
        # 支持前端传入自定义阈值进行实时预览
        custom_thresholds = body.get("thresholds")
        if custom_thresholds:
            result = bot.score_sunset(weather, thresholds=custom_thresholds)
        else:
            result = bot.score_sunset(weather)
        thresholds = custom_thresholds or bot.get_config("thresholds", {})
        result["weather"] = weather
        result["location"] = loc
        result["thresholds"] = thresholds
        result["data_source"] = source
        bus.publish("preview_done", result)
        json_response(self, result)

    def api_manual_push(self):
        """手动推送：无视评分阈值，直接生成晚霞文案并推送"""
        body = json.loads(read_body(self) or b"{}")
        loc_id = body.get("location_id")
        if loc_id is not None:
            loc_id = int(loc_id)
        
        # 如果没有指定地点，使用第一个启用的地点
        if not loc_id:
            locs = bot.get_locations(enabled_only=True)
            if not locs:
                json_response(self, {"ok": False, "error": "没有启用的地点"}, 400)
                return
            loc = locs[0]
        else:
            locs = {l["id"]: l for l in bot.get_locations()}
            if loc_id not in locs:
                json_response(self, {"ok": False, "error": "地点不存在"}, 404)
                return
            loc = locs[loc_id]
        
        # 获取天气数据（支持交叉验证）
        if bot.get_config("cross_validation", False):
            weather, err = bot.fetch_weather_multi(loc["lng"], loc["lat"])
            source = weather.get("_source_label", "multi") if weather else "multi(error)"
        else:
            weather, source, err = bot.fetch_weather_cached(loc["id"], loc["lng"], loc["lat"], force_refresh=False)
        if err and not weather:
            json_response(self, {"ok": False, "error": f"获取天气失败: {err}"}, 400)
            return
        
        # 计算评分
        score_result = bot.score_sunset(weather)
        
        # 获取日落时间
        sunset_time = weather.get("sunset_time", "")
        if not sunset_time:
            try:
                sunset_dt, _ = bot.get_sunset_time_for_location(loc["lng"], loc["lat"], weather=weather)
                sunset_time = sunset_dt.strftime("%H:%M") if sunset_dt else ""
            except Exception:
                pass
        
        # 生成推送文案（使用用户模板）
        text = bot.format_push_message(
            weather=weather,
            score_result=score_result,
            location_name=loc["name"],
            sunset_time=sunset_time,
            data_source=source,
        )
        
        # 无视评分阈值，直接推送
        push_results = bot.push_message(text)
        
        # 手动推送不记录到 push_history
        
        bot.audit("手动推送", loc["name"], f"手动推送 score={score_result['score']} channels={list(push_results.keys())}")
        bus.publish("manual_pushed", {"text": text, "results": push_results, "score": score_result["score"]})
        json_response(self, {
            "ok": True,
            "results": push_results,
            "score": score_result["score"],
            "text": text,
        })

    def api_test_push(self):
        """测试推送：向已配置的通道发送测试消息"""
        text = "🔔 晚霞推送测试消息\n如果你看到这条消息，说明推送通道配置正确！\n时间: " + datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        channels = ["wechat", "dingtalk", "feishu", "serverchan", "pushplus"]
        results = {}
        for ch in channels:
            if ch == "wechat":
                webhook = bot.get_credential("wechat_webhook", "")
                if webhook:
                    ok, msg = bot.push_wechat(text)
                    results[ch] = {"success": ok, "message": msg, "configured": True}
                else:
                    results[ch] = {"success": False, "message": "未配置 webhook", "configured": False}
            elif ch == "dingtalk":
                webhook = bot.get_credential("dingtalk_webhook", "")
                if webhook:
                    ok, msg = bot.push_dingtalk(text)
                    results[ch] = {"success": ok, "message": msg, "configured": True}
                else:
                    results[ch] = {"success": False, "message": "未配置 webhook", "configured": False}
            elif ch == "feishu":
                webhook = bot.get_credential("feishu_webhook", "")
                if webhook:
                    ok, msg = bot.push_feishu(text)
                    results[ch] = {"success": ok, "message": msg, "configured": True}
                else:
                    results[ch] = {"success": False, "message": "未配置 webhook", "configured": False}
            elif ch == "serverchan":
                key = bot.get_credential("serverchan_sendkey", "")
                if key:
                    ok, msg = bot.push_serverchan(text)
                    results[ch] = {"success": ok, "message": msg, "configured": True}
                else:
                    results[ch] = {"success": False, "message": "未配置 SendKey", "configured": False}
            elif ch == "pushplus":
                token = bot.get_credential("pushplus_token", "")
                if token:
                    ok, msg = bot.push_pushplus(text)
                    results[ch] = {"success": ok, "message": msg, "configured": True}
                else:
                    results[ch] = {"success": False, "message": "未配置 Token", "configured": False}
        # 测试推送不记录到 push_history
        bus.publish("test_pushed", results)
        json_response(self, {"ok": True, "results": results})

    def api_test_full(self):
        """测试：获取天气 + 评分（不推送，仅查看数据和评分结果）"""
        body = json.loads(read_body(self) or b"{}")
        loc_id = body.get("location_id")
        if loc_id is not None:
            loc_id = int(loc_id)
        
        # 如果没有指定地点，使用第一个启用的地点
        if not loc_id:
            locs = bot.get_locations(enabled_only=True)
            if not locs:
                json_response(self, {"ok": False, "error": "没有启用的地点"}, 400)
                return
            loc = locs[0]
        else:
            locs = {l["id"]: l for l in bot.get_locations()}
            if loc_id not in locs:
                json_response(self, {"ok": False, "error": "地点不存在"}, 404)
                return
            loc = locs[loc_id]
        
        result = {"location": loc, "steps": {}}
        
        # Step 1: 获取天气数据（使用缓存，不消耗 API 次数）
        weather, source, err = bot.fetch_weather_cached(loc["id"], loc["lng"], loc["lat"], force_refresh=False)
        if err and not weather:
            result["steps"]["fetch"] = {"success": False, "error": err}
            result["ok"] = False
            json_response(self, result)
            return
        result["steps"]["fetch"] = {"success": True, "data": weather, "source": source}
        
        # Step 2: 计算评分
        score_result = bot.score_sunset(weather)
        result["steps"]["score"] = {
            "success": True, 
            "score": score_result["score"],
            "breakdown": score_result["breakdown"],
            "recommend": score_result["recommend"]
        }
        
        # Step 3: 生成推送文案预览（不实际推送）
        sunset_time = weather.get("sunset_time", "")
        if not sunset_time:
            try:
                sunset_dt, _ = bot.get_sunset_time_for_location(loc["lng"], loc["lat"], weather=weather)
                sunset_time = sunset_dt.strftime("%H:%M") if sunset_dt else ""
            except Exception:
                pass
        text = bot.format_push_message(
            weather=weather,
            score_result=score_result,
            location_name=loc["name"],
            sunset_time=sunset_time,
            data_source=source,
        )
        result["steps"]["preview"] = {"text": text}
        
        result["ok"] = True
        bus.publish("test_full_done", result)
        json_response(self, result)

    # ============ 数据核验 ============
    def api_verify_data(self):
        """获取指定地点的完整 API 数据 + 预期推送文案（用于核验）"""
        from urllib.parse import urlparse, parse_qs
        qs = parse_qs(urlparse(self.path).query)
        loc_id = int(qs.get("location_id", [0])[0])
        if not loc_id:
            locs = bot.get_locations(enabled_only=True)
            if not locs:
                json_response(self, {"ok": False, "error": "没有启用的地点"}, 400)
                return
            loc_id = locs[0]["id"]
        locs = {l["id"]: l for l in bot.get_locations()}
        if loc_id not in locs:
            json_response(self, {"error": "地点不存在"}, 404)
            return
        loc = locs[loc_id]
        # 使用缓存数据（不触发 API 调用）
        weather, source, err = bot.fetch_weather_cached(loc["id"], loc["lng"], loc["lat"], force_refresh=False)
        if not weather:
            json_response(self, {"ok": False, "error": err or "无缓存数据"}, 400)
            return
        # 评分
        score_result = bot.score_sunset(weather)
        # 日落时间
        sunset_time = weather.get("sunset_time", "") or weather.get("sunset", "")
        if not sunset_time:
            try:
                sunset_dt, _ = bot.get_sunset_time_for_location(loc["lng"], loc["lat"], weather=weather)
                sunset_time = sunset_dt.strftime("%H:%M") if sunset_dt else ""
            except Exception:
                pass
        # 生成预期推送文案
        expected_text = bot.format_push_message(weather, score_result, loc["name"], sunset_time, source)
        # 返回完整原始数据 + 预期文案
        json_response(self, {
            "ok": True,
            "location": loc["name"],
            "location_id": loc_id,
            "source": source,
            "score": score_result["score"],
            "recommend": score_result["recommend"],
            "breakdown": score_result.get("breakdown", {}),
            "sunset_time": sunset_time,
            "raw_weather": weather,
            "expected_push_text": expected_text,
        })

    def api_verify_compare(self):
        """对比用户粘贴的实际推送消息中的数值与 API 原始数据"""
        import re
        body = json.loads(read_body(self) or b"{}")
        loc_id = body.get("location_id")
        actual_text = body.get("actual_text", "").strip()
        if not actual_text:
            json_response(self, {"ok": False, "error": "请粘贴收到的推送消息"}, 400)
            return
        if not loc_id:
            locs = bot.get_locations(enabled_only=True)
            if not locs:
                json_response(self, {"ok": False, "error": "没有启用的地点"}, 400)
                return
            loc_id = locs[0]["id"]
        else:
            loc_id = int(loc_id)
        locs = {l["id"]: l for l in bot.get_locations()}
        if loc_id not in locs:
            json_response(self, {"error": "地点不存在"}, 404)
            return
        loc = locs[loc_id]
        # 获取缓存数据
        weather, source, err = bot.fetch_weather_cached(loc["id"], loc["lng"], loc["lat"], force_refresh=False)
        if not weather:
            json_response(self, {"ok": False, "error": err or "无缓存数据"}, 400)
            return
        score_result = bot.score_sunset(weather)
        sunset_time = weather.get("sunset_time", "") or weather.get("sunset", "")
        if not sunset_time:
            try:
                sunset_dt, _ = bot.get_sunset_time_for_location(loc["lng"], loc["lat"], weather=weather)
                sunset_time = sunset_dt.strftime("%H:%M") if sunset_dt else ""
            except Exception:
                pass
        # 逐项对比：从实际消息中提取数值，与 API 数据对比
        checks = []
        def extract_number(text, patterns):
            """从文本中提取数值，支持多种格式"""
            for pat in patterns:
                m = re.search(pat, text)
                if m:
                    try:
                        return float(m.group(1))
                    except (ValueError, IndexError):
                        pass
            return None
        # 定义要核验的字段（覆盖所有数值型数据）
        fields = [
            {"key": "score", "label": "评分", "api_val": score_result["score"],
             "patterns": [r'(?:评分|分数)[:\s]*(\d+(?:\.\d+)?)', r'(\d+(?:\.\d+)?)\s*分']},
            {"key": "temperature", "label": "温度", "api_val": weather.get("temperature"),
             "patterns": [r'(?:温度|气温)[:\s]*(-?\d+(?:\.\d+)?)', r'(-?\d+(?:\.\d+)?)\s*°']},
            {"key": "apparent_temperature", "label": "体感温度", "api_val": weather.get("apparent_temperature"),
             "patterns": [r'体感[:\s]*(-?\d+(?:\.\d+)?)']},
            {"key": "cloudrate", "label": "总云量", "api_val": weather.get("cloudrate"),
             "patterns": [r'(?:总云量|云量)[:\s]*(\d+(?:\.\d+)?)', r'(\d+(?:\.\d+)?)\s*%.*云']},
            {"key": "sunset_cloudrate", "label": "日落云量", "api_val": weather.get("sunset_cloudrate"),
             "patterns": [r'(?:日落.*?云量|日落时.*?云)[:\s]*(\d+(?:\.\d+)?)']},
            {"key": "humidity", "label": "湿度", "api_val": weather.get("humidity"),
             "patterns": [r'(?:湿度)[:\s]*(\d+(?:\.\d+)?)']},
            {"key": "wind", "label": "风速", "api_val": weather.get("wind"),
             "patterns": [r'(?:风速|风力)[:\s]*(\d+(?:\.\d+)?)']},
            {"key": "visibility", "label": "能见度", "api_val": weather.get("visibility"),
             "patterns": [r'(?:能见度|可视)[:\s]*(\d+(?:\.\d+)?)']},
            {"key": "precipitation", "label": "降水", "api_val": weather.get("precipitation"),
             "patterns": [r'(?:降水|降雨|降水强度)[:\s]*(\d+(?:\.\d+)?)']},
            {"key": "aqi", "label": "空气质量(国标)", "api_val": weather.get("aqi"),
             "patterns": [r'(?:AQI|空气质量)[:\s]*(\d+(?:\.\d+)?)']},
            {"key": "pm25", "label": "PM2.5", "api_val": weather.get("pm25"),
             "patterns": [r'PM2\.5[:\s]*(\d+(?:\.\d+)?)']},
            {"key": "pm10", "label": "PM10", "api_val": weather.get("pm10"),
             "patterns": [r'PM10[:\s]*(\d+(?:\.\d+)?)']},
            {"key": "o3", "label": "臭氧", "api_val": weather.get("o3"),
             "patterns": [r'(?:臭氧|O3)[:\s]*(\d+(?:\.\d+)?)']},
            {"key": "pressure", "label": "气压", "api_val": weather.get("pressure"),
             "patterns": [r'(?:气压|气压)[:\s]*(\d+(?:\.\d+)?)']},
            {"key": "wind_direction", "label": "风向", "api_val": weather.get("wind_direction"),
             "patterns": [r'(?:风向|风向角度)[:\s]*(\d+(?:\.\d+)?)']},
        ]
        match_count = 0
        total_checked = 0
        for f in fields:
            api_val = f["api_val"]
            if api_val is None or api_val == "?" or api_val == "":
                continue
            actual_val = extract_number(actual_text, f["patterns"])
            if actual_val is None:
                continue
            total_checked += 1
            api_num = float(api_val) if not isinstance(api_val, (int, float)) else api_val
            matched = abs(api_num - actual_val) < 0.5
            if matched:
                match_count += 1
            checks.append({
                "field": f["key"],
                "label": f["label"],
                "api_value": api_num,
                "actual_value": actual_val,
                "match": matched,
            })
        # 检查地点名称
        loc_match = loc["name"] in actual_text
        # 检查日落时间
        sunset_match = False
        if sunset_time and sunset_time in actual_text:
            sunset_match = True
        # 检查推荐标签
        recommend_match = False
        if score_result["recommend"]:
            recommend_match = "✅" in actual_text or "推荐" in actual_text
        else:
            recommend_match = "⚠️" in actual_text or "一般" in actual_text or "不推荐" in actual_text
        # 检查数据来源
        source_match = source in actual_text if source else None
        # 整体评估
        overall_score = (match_count / total_checked * 100) if total_checked > 0 else 0
        json_response(self, {
            "ok": True,
            "summary": {
                "total_checked": total_checked,
                "match_count": match_count,
                "overall_score": round(overall_score),
                "location_match": loc_match,
                "sunset_match": sunset_match,
                "recommend_match": recommend_match,
                "source_match": source_match,
            },
            "checks": checks,
            "actual_text": actual_text,
        })

    # ============ SSE ============
    def api_sse(self):
        """Server-Sent Events"""
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Connection", "keep-alive")
        self.send_header("X-Accel-Buffering", "no")
        self.end_headers()
        # 订阅
        q = bus.subscribe()
        # retry: 告诉客户端断线后 3 秒自动重连
        self.wfile.write(b"retry: 3000\n")
        # 立即推一条 hello
        self.wfile.write(b"data: " + json.dumps({"type": "hello", "time": time.time()}).encode() + b"\n\n")
        self.wfile.flush()
        try:
            last_ping = time.time()
            while True:
                try:
                    msg = q.get(timeout=10)
                    self.wfile.write(b"data: " + msg.encode() + b"\n\n")
                    self.wfile.flush()
                except queue.Empty:
                    # 30s 心跳
                    if time.time() - last_ping > 20:
                        self.wfile.write(b": ping\n\n")
                        self.wfile.flush()
                        last_ping = time.time()
        except (BrokenPipeError, ConnectionResetError, OSError):
            pass
        finally:
            bus.unsubscribe(q)

    def api_get_sunset_photos(self):
        """获取历史晚霞照片分析记录"""
        try:
            conn = bot.get_db()
            rows = conn.execute(
                """SELECT id, location_id, predicted_score, ai_score, ai_cloud_coverage,
                          ai_color_saturation, ai_description, calibration_offset, created_at
                   FROM sunset_photos ORDER BY created_at DESC LIMIT 50"""
            ).fetchall()
            conn.close()
            photos = [dict(r) for r in rows]
        except Exception:
            photos = []
        # 获取当前校准偏移量
        current_offset = bot.get_calibration_offset()
        json_response(self, {"photos": photos, "current_offset": current_offset})

    def api_clear_sunset_photos(self):
        """清除所有晚霞照片分析记录"""
        try:
            conn = bot.get_db()
            conn.execute("DELETE FROM sunset_photos")
            conn.commit()
            conn.close()
            bot.audit("AI分析记录", "全部", "已清除")
            json_response(self, {"ok": True})
        except Exception as e:
            json_response(self, {"error": str(e)}, 500)

    def api_sunset_photo_upload(self):
        """上传晚霞照片并调用 AI 分析"""
        content_type = self.headers.get("Content-Type", "")
        body = read_body(self)
        fields, files = parse_multipart(self, content_type, body)

        if not files:
            json_response(self, {"error": "无照片文件"}, 400)
            return

        # 获取表单字段
        location_id = fields.get("location_id")
        try:
            location_id = int(location_id) if location_id else None
        except (ValueError, TypeError):
            location_id = None

        # 取第一张图片
        file_data = list(files.values())[0]
        image_bytes = file_data["content"]
        filename = file_data["filename"]

        # 限制文件大小 (最大 5MB)
        if len(image_bytes) > 5 * 1024 * 1024:
            json_response(self, {"error": "照片过大，请压缩到 5MB 以内"}, 400)
            return

        # 自动读取 EXIF 拍摄时间
        photo_datetime = _extract_exif_datetime(image_bytes)
        if not photo_datetime:
            from datetime import datetime
            photo_datetime = datetime.now().strftime("%Y-%m-%d %H:%M")
        # 提取日期部分用于查询推送历史
        photo_date = photo_datetime.split(" ")[0]
        bot.log.info(f"[AI校准] 照片拍摄时间: {photo_datetime}")

        # 自动查询当天推送评分
        predicted_score = None
        if location_id:
            predicted_score = bot.get_push_score_by_date(location_id, photo_date)
            if predicted_score is not None:
                bot.log.info(f"[AI校准] 自动获取当天推送评分: {predicted_score}")
            else:
                bot.log.info(f"[AI校准] 未找到 {photo_date} 的推送记录")

        # 转 base64
        photo_base64 = base64.b64encode(image_bytes).decode("utf-8")

        # 调用 AI 分析
        ai_result = bot.analyze_sunset_photo(photo_base64)
        if not ai_result:
            json_response(self, {"error": "AI 分析失败，请检查 zhipu_key 配置或稍后重试"}, 500)
            return

        # 保存分析结果
        offset = bot.save_sunset_photo(location_id, predicted_score, ai_result, filename, photo_datetime)

        # 获取最新的中位数偏移
        new_offset = bot.get_calibration_offset(location_id)

        bot.audit("AI晚霞分析", str(location_id),
                  f"时间={photo_datetime} AI评分={ai_result.get('ai_score')} 预测={predicted_score} 偏移={offset}")

        json_response(self, {
            "ok": True,
            "photo_datetime": photo_datetime,
            "predicted_score": predicted_score,
            "ai_result": ai_result,
            "this_offset": offset,
            "calibration_offset": new_offset,
        })

    def api_set_score_offset(self):
        """设置手动评分偏移量"""
        body = json.loads(read_body(self) or b"{}")
        offset = body.get("score_offset", 0)
        try:
            offset = int(offset)
            offset = max(-20, min(20, offset))
        except (ValueError, TypeError):
            offset = 0
        bot.set_config("score_offset", str(offset))
        bot.audit("评分偏移设置", None, f"score_offset={offset}")
        json_response(self, {"ok": True, "score_offset": offset})

    # ============ 终端文件传输 ============

    def api_terminal_upload(self):
        """上传文件到服务器指定目录"""
        content_type = self.headers.get("Content-Type", "")
        body = read_body(self)
        fields, files = parse_multipart(self, content_type, body)
        target_dir = fields.get("path", str(bot.BASE_DIR))
        # 安全检查：限制在 BASE_DIR 范围内
        target_path = Path(target_dir).resolve()
        if not str(target_path).startswith(str(bot.BASE_DIR.resolve())):
            json_response(self, {"error": "路径不允许超出服务目录"}, 403)
            return
        if not files:
            json_response(self, {"error": "未收到文件"}, 400)
            return
        saved = []
        for name, file_data in files.items():
            dest = target_path / file_data["filename"]
            dest.write_bytes(file_data["content"])
            saved.append(file_data["filename"])
            bot.log.info(f"[terminal] 上传文件: {dest}")
        bot.audit("文件上传", ", ".join(saved), f"上传到 {target_dir}")
        json_response(self, {"ok": True, "files": saved, "path": str(target_path)})

    def api_terminal_download(self):
        """从服务器下载指定文件"""
        from urllib.parse import parse_qs, urlparse
        qs = parse_qs(urlparse(self.path).query)
        file_path = qs.get("path", [""])[0]
        if not file_path:
            json_response(self, {"error": "缺少 path 参数"}, 400)
            return
        target = Path(file_path).resolve()
        # 安全检查
        if not str(target).startswith(str(bot.BASE_DIR.resolve())):
            json_response(self, {"error": "路径不允许超出服务目录"}, 403)
            return
        if not target.is_file():
            json_response(self, {"error": f"文件不存在: {file_path}"}, 404)
            return
        import mimetypes
        mime = mimetypes.guess_type(str(target))[0] or "application/octet-stream"
        self.send_response(200)
        self.send_header("Content-Type", mime)
        self.send_header("Content-Disposition", f'attachment; filename="{target.name}"')
        self.send_header("Content-Length", str(target.stat().st_size))
        self.end_headers()
        with open(target, "rb") as f:
            while True:
                chunk = f.read(8192)
                if not chunk:
                    break
                self.wfile.write(chunk)
        bot.log.info(f"[terminal] 下载文件: {target}")

    def api_filebrowser_list(self):
        """列出指定目录内容（文件浏览器）"""
        from urllib.parse import parse_qs, urlparse
        qs = parse_qs(urlparse(self.path).query)
        dir_path = qs.get("path", ["/"])[0]
        target = Path(dir_path).resolve()
        # 安全检查：允许访问整个文件系统（根目录 /），但禁止危险路径
        blocked = ["/proc", "/sys", "/dev"]
        if any(str(target).startswith(b) for b in blocked):
            json_response(self, {"error": "禁止访问该路径"}, 403)
            return
        if not target.is_dir():
            json_response(self, {"error": f"不是目录: {dir_path}"}, 400)
            return
        items = []
        try:
            for entry in sorted(target.iterdir(), key=lambda e: (not e.is_dir(), e.name.lower())):
                try:
                    items.append({
                        "name": entry.name,
                        "is_dir": entry.is_dir(),
                        "size": entry.stat().st_size if entry.is_file() else 0,
                        "mtime": entry.stat().st_mtime,
                    })
                except (PermissionError, OSError):
                    pass
        except PermissionError:
            json_response(self, {"error": "无权限访问"}, 403)
            return
        json_response(self, {
            "ok": True,
            "path": str(target),
            "parent": str(target.parent) if target != Path(target.anchor) else None,
            "items": items,
        })

    # ============ WebSocket 终端（由独立 WS 服务器提供，见下方）============


# ==================== WebSocket 终端服务器（websockets 12.x，端口 5001）====================

async def _ws_terminal_handler(websocket, path):
    """处理单个 WebSocket 终端连接"""
    import asyncio
    from urllib.parse import parse_qs
    qs = parse_qs(urlparse(path).query)
    token_list = qs.get("session", []) or qs.get("ws_token", [])
    token = token_list[0] if token_list else ""
    bot.log.info(f"[ws-terminal] path={path!r} token={token[:20] if token else '(empty)'}...")
    # 支持 session cookie token 或 WS 短期 token
    authed = False
    if token:
        authed = verify_session(token) or (token in _ws_tokens and _ws_tokens[token] > time.time())
    if not authed:
        bot.log.warning(f"[ws-terminal] 认证失败 token_len={len(token)}")
        await websocket.close(4001, "未登录")
        return
    bot.log.info("[ws-terminal] 连接建立")
    import pty, fcntl, termios
    try:
        master_fd, slave_fd = pty.openpty()
        fcntl.ioctl(master_fd, termios.TIOCSWINSZ, struct.pack("HHHH", 32, 100, 0, 0))
        env = os.environ.copy()
        env.update({
            "TERM": "xterm-256color",
            "LANG": "en_US.UTF-8",
            "HOME": "/root",
            "PS1": f"root@$(hostname):{bot.BASE_DIR}$ ",
        })
        # 子进程：setsid 创建新会话 + TIOCSCTTY 绑定控制终端（消除 can't access tty）
        def _pty_preexec():
            os.setsid()
            try:
                fcntl.ioctl(slave_fd, termios.TIOCSCTTY, 0)
            except Exception:
                pass
        shell = subprocess.Popen(["bash", "-i"], stdin=slave_fd, stdout=slave_fd, stderr=slave_fd,
                                  preexec_fn=_pty_preexec, env=env, cwd=str(bot.BASE_DIR))
        os.close(slave_fd)
        fd = master_fd
    except Exception as e:
        bot.log.error(f"[ws-terminal] PTY 失败: {e}")
        await websocket.close(1011, str(e))
        return
    bot.log.info(f"[ws-terminal] shell pid={shell.pid}")
    loop = asyncio.get_event_loop()
    # PTY → WebSocket（后台线程）
    def _pty_reader():
        try:
            while True:
                r, _, _ = _select.select([fd], [], [], 0.3)
                if r:
                    try:
                        data = os.read(fd, 4096)
                        if not data: break
                        asyncio.run_coroutine_threadsafe(websocket.send(data), loop).result(timeout=2)
                    except OSError: break
                    except Exception: break
                if shell.poll() is not None:
                    try:
                        r2, _, _ = _select.select([fd], [], [], 0.1)
                        if r2:
                            data = os.read(fd, 4096)
                            if data:
                                asyncio.run_coroutine_threadsafe(websocket.send(data), loop).result(timeout=2)
                    except Exception: pass
                    break
        except Exception: pass
    reader_thread = threading.Thread(target=_pty_reader, daemon=True)
    reader_thread.start()
    # WebSocket → PTY
    try:
        async for message in websocket:
            if isinstance(message, str) and message.startswith('{"type"'):
                try:
                    ctrl = json.loads(message)
                    if ctrl.get("type") == "resize":
                        cols = int(ctrl.get("cols", 100))
                        rows = int(ctrl.get("rows", 32))
                        fcntl.ioctl(fd, termios.TIOCSWINSZ, struct.pack("HHHH", rows, cols, 0, 0))
                        continue
                except Exception: pass
            if isinstance(message, str):
                os.write(fd, message.encode("utf-8"))
            elif isinstance(message, bytes):
                os.write(fd, message)
    except Exception as e:
        bot.log.warning(f"[ws-terminal] 异常: {e}")
    finally:
        try: shell.terminate()
        except Exception: pass
        try: os.close(fd)
        except Exception: pass
        bot.log.info("[ws-terminal] 连接关闭")

def _run_ws_server(host, port):
    """在独立线程中运行 WebSocket 服务器"""
    import asyncio
    import websockets
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    start_server = websockets.serve(_ws_terminal_handler, host, port, max_size=2**20, ping_interval=30, ping_timeout=10)
    bot.log.info(f"WebSocket 终端: ws://{host}:{port}")
    try:
        loop.run_until_complete(start_server)
        loop.run_forever()
    except Exception as e:
        bot.log.error(f"[ws-terminal] 服务器异常: {e}")
    finally:
        loop.close()


# ==================== 启动 ====================

def _schedule_runner():
    """后台线程：定期执行 web.py 中注册的 schedule 任务"""
    while True:
        try:
            schedule.run_pending()
        except Exception as e:
            bot.log.warning(f"[schedule] 执行异常: {e}")
        time.sleep(1)


def run(host="0.0.0.0", port=5000):
    bot.init_db()
    _load_proxy_from_config()
    _restore_scheduled_pushes()
    t = threading.Thread(target=_schedule_runner, daemon=True)
    t.start()
    # WebSocket 终端服务器（端口 5001）
    ws_port = port + 1
    ws_thread = threading.Thread(target=_run_ws_server, args=(host, ws_port), daemon=True)
    ws_thread.start()
    time.sleep(0.5)
    server = ThreadingHTTPServer((host, port), Handler)
    bot.log.info(f"Web 启动: http://{host}:{port}  WS终端: ws://{host}:{ws_port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.shutdown()


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    run(port=port)
