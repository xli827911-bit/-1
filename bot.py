#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
sunset-bot v6 - bot.py
核心：调度 / 推送编排 / 升级
设计：每个函数独立可测、备份默认时间戳、所有 IO 都有 timeout

模块结构：
  providers.py  — 天气 Provider 抽象层（13 个实现 + 注册表 + 降级链）
  scoring.py    — 晚霞评分算法 v3（多层云量 + AI 校准）
  pusher.py     — 推送通道（5 通道 + 并行推送 + 富文本 + Webhook 预检）
  bot.py        — 薄编排层，re-export 保持向后兼容

bot.py 内部分布：
  常量 + HTTP Session（含自动重试）
  from providers/scoring/pusher import（天气/评分/推送模块）
  路径常量 / 推送模板 / 云层分类字典
  日志（TimedRotatingFileHandler，自动轮转 30 天）
  数据库（SQLite WAL + 线程本地连接 + schema 版本迁移）
  天气缓存（TTL 1h 自动过期）
  凭证/地点/配置管理
  AI 照片分析（Gemini/智谱）
  天气 API 调用（自动降级 + token 缓存）
  日落时间计算
  推送历史 / 审计日志 / 自动清理
  多源并行天气验证（ThreadPoolExecutor）
  配置导入导出 / 版本管理 / 调度器 / main()
"""
import os
import sys
import json
import time
import signal
import sqlite3
import logging
import hashlib
import shutil
import threading
import subprocess
import re
from datetime import datetime, timedelta
from pathlib import Path

import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import schedule

# ==================== 常量定义 ===================
APP_VERSION = "6.0.0"  # 当前应用版本（每次发布新版本时递增）

# 超时设置（秒）
API_TIMEOUT = 10          # 天气 API 请求默认超时
PUSH_TIMEOUT = 10         # 推送通道请求超时
DB_TIMEOUT = 10           # SQLite 连接超时
AI_TIMEOUT = 30           # AI 视觉 API 超时

# 分页默认值
DEFAULT_PUSH_HISTORY_LIMIT = 50
DEFAULT_AUDIT_LOG_LIMIT = 100
AUDIT_LOG_AUTO_CLEAN_THRESHOLD = 50  # 每写入 N 条自动清理
AUDIT_LOG_MAX_COUNT = 500            # 最大保留条数

# HTTP 状态码
HTTP_OK = 200
HTTP_BAD_REQUEST = 400
HTTP_UNAUTHORIZED = 401
HTTP_NOT_FOUND = 404
HTTP_INTERNAL_ERROR = 500

# 共享 HTTP Session（连接池复用 + 自动重试）
_http_session = requests.Session()
# 配置自动重试：最多重试 3 次，仅对 5xx 和连接错误重试
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
_retry_strategy = Retry(
    total=3,
    backoff_factor=1,  # 1s, 2s, 4s 指数退避
    status_forcelist=[500, 502, 503, 504],
    allowed_methods=["GET", "POST"],
)
_http_session.mount("https://", HTTPAdapter(max_retries=_retry_strategy))
_http_session.mount("http://", HTTPAdapter(max_retries=_retry_strategy))

def get_http_session():
    """获取共享 HTTP Session（带自动重试）"""
    return _http_session

# ==================== 天气 Provider（从 providers.py 导入） ===================
from providers import *  # noqa: F401,F403



def get_weather_provider():
    """获取当前配置的天气 API 提供者（默认彩云）"""
    provider_name = get_config("weather_provider", "caiyun")
    return WEATHER_PROVIDERS.get(provider_name, WEATHER_PROVIDERS["caiyun"])


def get_available_providers():
    """获取所有可用的天气 API 提供者列表"""
    return [{"name": p.name, "display_name": p.display_name} for p in WEATHER_PROVIDERS.values()]


# ==================== 路径常量 ====================
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
LOGS_DIR = DATA_DIR / "logs"
BACKUP_DIR = BASE_DIR / "backups"
VERSIONS_DIR = BASE_DIR / "_versions"
DB_PATH = DATA_DIR / "state.db"
PID_FILE = DATA_DIR / "bot.pid"
SECRET_KEY_FILE = DATA_DIR / ".secret_key"

# 默认阈值（晚霞评分 0~100）
DEFAULT_THRESHOLDS = {
    "cloudrate_min": 30,      # 最低云量 (%)
    "cloudrate_max": 85,      # 最高云量 (%)
    "humidity_min": 20,       # 最低湿度 (%)
    "humidity_max": 80,       # 最高湿度 (%)
    "wind_max": 6,            # 最高风速 (m/s)
    "visibility_min": 8,      # 最低能见度 (km)
    "score_threshold": 50,    # 推荐阈值
}

# 不可见晚霞的天气现象（雾/霾/雨雪等直接遮挡日落的现象）
BLOCKED_SKYCONS = {
    "FOG", "LIGHT_HAZE", "MODERATE_HAZE", "HEAVY_HAZE",
    "LIGHT_RAIN", "MODERATE_RAIN", "HEAVY_RAIN", "STORM_RAIN",
    "LIGHT_SNOW", "MODERATE_SNOW", "HEAVY_SNOW", "STORM_SNOW",
    "DUST", "SAND",
}
# 对应的中文关键词（用于从翻译后的 skycon 文本判断）
BLOCKED_SKYCON_ZH = {"雾", "霾", "雨", "雪", "尘", "沙"}

# 晚霞科学分类（v2: 支持多层云量精确判断，向后兼容总云量）
def _has_multi(w):
    """是否有多层云量数据"""
    return any(w.get(k) is not None for k in ("cloud_low", "cloud_mid", "cloud_high"))

SUNSET_CLOUD_TYPES = {
    "fog_haze": {
        "name": "雾/霾遮蔽",
        "desc": "天空被雾气或霾层覆盖，日落不可见或极度模糊",
        "condition": lambda w: _is_skycon_blocked(w),
        "tip": "今天看不到清晰日落，建议室内活动或等待明天",
    },
    "low_cloud_blocked": {
        "name": "低云遮蔽",
        "desc": "低层云量过厚（>80%），阳光无法穿透，日落不可见",
        "condition": lambda w: _has_multi(w) and (w.get("cloud_low", 0) or 0) > 80,
        "tip": "低云层太厚遮挡了日落，今天不适合拍晚霞",
    },
    "overcast": {
        "name": "厚云遮日",
        "desc": "总云量过高（>90%），阳光完全被遮挡，无晚霞",
        "condition": lambda w: not _has_multi(w) and w.get("cloudrate", 0) > 90,
        "tip": "厚云层完全遮挡日落，今天不适合拍晚霞",
    },
    "cirrus": {
        "name": "卷积云丝缕晚霞",
        "desc": "细如发丝的高空云，淡粉淡蓝渐变",
        "condition": lambda w: (
            (_has_multi(w) and (w.get("cloud_high", 0) or 0) > 30 and (w.get("cloud_low", 0) or 0) < 30 and w.get("visibility", 0) > 10)
            or (not _has_multi(w) and 10 <= w.get("cloudrate", 0) <= 35 and w.get("visibility", 0) > 15)
        ),
        "tip": "搭配湖面、草坪、树枝框架构图，适合治愈风手机随拍",
    },
    "altocumulus": {
        "name": "高积云鱼鳞晚霞",
        "desc": "细碎鱼鳞状云片，粉紫+橘金渐变",
        "condition": lambda w: (
            (_has_multi(w) and 30 <= (w.get("cloud_mid", 0) or 0) <= 70 and (w.get("cloud_low", 0) or 0) < 40)
            or (not _has_multi(w) and 40 <= w.get("cloudrate", 0) <= 70 and 4 < w.get("wind", 0) < 8)
        ),
        "tip": "楼顶、开阔平原机位，广角拍整片天空；日落前10分钟最佳",
    },
    "fire_sky": {
        "name": "火烧云晚霞",
        "desc": "大红橘红铺满天际，亮度最高",
        "condition": lambda w: (
            (_has_multi(w) and (w.get("cloud_high", 0) or 0) > 50 and (w.get("cloud_mid", 0) or 0) > 20 and (w.get("cloud_low", 0) or 0) < 50 and w.get("precipitation", 0) == 0)
            or (not _has_multi(w) and w.get("cloudrate", 0) > 30 and w.get("visibility", 0) > 10 and w.get("precipitation", 0) == 0 and w.get("humidity", 0) < 70)
        ),
        "tip": "日落瞬间黄金5分钟，优先西向无遮挡视野；水面倒影翻倍出片",
    },
    "stratocumulus": {
        "name": "层积云晚霞",
        "desc": "大面积平铺碎云，橙红铺满半边天",
        "condition": lambda w: (
            (_has_multi(w) and 30 <= (w.get("cloud_mid", 0) or 0) <= 60 and (w.get("cloud_low", 0) or 0) < 30 and w.get("wind", 0) < 4)
            or (not _has_multi(w) and 30 <= w.get("cloudrate", 0) <= 60 and w.get("wind", 0) < 4)
        ),
        "tip": "适合散步、江边/湖边慢走，光线柔和拍人像不逆光",
    },
    "stratus": {
        "name": "层云整片晚霞",
        "desc": "整片天空统一橘粉色，低饱和治愈",
        "condition": lambda w: (
            (_has_multi(w) and 40 <= (w.get("cloud_low", 0) or 0) <= 70 and (w.get("cloud_mid", 0) or 0) < 40 and w.get("wind", 0) < 3)
            or (not _has_multi(w) and 50 <= w.get("cloudrate", 0) <= 90 and w.get("wind", 0) < 3)
        ),
        "tip": "城市街道、天台、海边栈道，不用找复杂机位，随手拍都好看",
    },
    "cumulonimbus": {
        "name": "积雨云火烧晚霞",
        "desc": "雨后厚云边缘赤红，明暗对比极强",
        "condition": lambda w: w.get("cloudrate", 0) > 70 and w.get("precipitation", 0) > 0 and w.get("temperature", 0) > 25,
        "tip": "雨后半小时内抓紧拍摄，长焦拍云层层次，氛围感大片",
    },
    "blue_hour": {
        "name": "粉紫暮霭晚霞",
        "desc": "蓝调时刻，下半粉橘上半淡紫蓝，冷暖撞色",
        "condition": lambda w: w.get("cloudrate", 0) < 40 and w.get("humidity", 0) > 60 and w.get("visibility", 0) > 10,
        "tip": "日落后10-20分钟拍摄，打开夜景模式，搭配路灯、建筑剪影",
    },
    "golden": {
        "name": "金粉薄暮",
        "desc": "高空薄云折射阳光，整片天空淡金色",
        "condition": lambda w: (
            (_has_multi(w) and 20 <= (w.get("cloud_high", 0) or 0) <= 60 and (w.get("cloud_low", 0) or 0) < 20 and w.get("visibility", 0) > 15)
            or (not _has_multi(w) and 5 <= w.get("cloudrate", 0) <= 30 and w.get("visibility", 0) > 20)
        ),
        "tip": "爬山、高地观景台，远景城市轮廓+金边天空",
    },
}

# 默认推送文案模板（傍晚晚霞预报）
DEFAULT_PUSH_TEMPLATE = """✨ 晚霞播报 · {location}
⭐ 评分: {score}/100 ({recommend_tag})
🌇 日落时间: {sunset_time}
🌡 温度: {temperature}°C | 💧 湿度: {humidity}%
☁ 云量: {cloudrate}% (高{cloud_high}/中{cloud_mid}/低{cloud_low})
💨 风速: {wind}m/s | 👁 能见度: {visibility}km
🌫 PM2.5: {pm25} | 🏭 PM10: {pm10} | 🌪 沙尘: {dust} | 🫁 AQI: {us_aqi}
🎨 云型: {cloud_type}
📷 {cloud_tip}
📦 数据: {data_source}"""

# 早间全天播报模板
DEFAULT_MORNING_TEMPLATE = """🌤 早间天气播报 · {location}
🗓 {skycon}
🌡 温度: {temp_low}°C ~ {temp_high}°C
🌅 日出: {sunrise} | 🌇 日落: {sunset_time}
💧 湿度: {humidity}% | ☁ 云量: {cloudrate}%
💨 风速: {wind}m/s | 👁 能见度: {visibility}km
🌧 降水概率: {precip_probability}%
☀ 紫外线: {uv_desc} | 🫁 AQI: {aqi}
👔 穿衣: {dressing} | 🚗 洗车: {car_washing}
🤧 感冒: {cold_risk} | 😌 舒适: {comfort_desc}
⭐ 晚霞预估: {score}/100 ({recommend_tag})
📝 {forecast_keypoint}
📦 数据: {data_source}"""

# 推送模板可用变量
PUSH_VARIABLES = [
    {"key": "{location}", "desc": "地点名称"},
    {"key": "{score}", "desc": "晚霞评分"},
    {"key": "{recommend_tag}", "desc": "推荐标签"},
    {"key": "{sunset_time}", "desc": "日落时间"},
    {"key": "{sunrise}", "desc": "日出时间"},
    {"key": "{temperature}", "desc": "当前温度"},
    {"key": "{apparent_temperature}", "desc": "体感温度"},
    {"key": "{temp_high}", "desc": "最高温度"},
    {"key": "{temp_low}", "desc": "最低温度"},
    {"key": "{humidity}", "desc": "湿度"},
    {"key": "{cloudrate}", "desc": "总云量"},
    {"key": "{sunset_cloudrate}", "desc": "日落时云量"},
    {"key": "{cloud_high}", "desc": "高云量"},
    {"key": "{cloud_mid}", "desc": "中云量"},
    {"key": "{cloud_low}", "desc": "低云量"},
    {"key": "{wind}", "desc": "风速"},
    {"key": "{visibility}", "desc": "能见度"},
    {"key": "{precip_probability}", "desc": "降水概率"},
    {"key": "{skycon}", "desc": "天气现象"},
    {"key": "{forecast_keypoint}", "desc": "预报摘要"},
    {"key": "{uv_desc}", "desc": "紫外线"},
    {"key": "{aqi}", "desc": "空气质量(国标)"},
    {"key": "{aqi_usa}", "desc": "空气质量(美标)"},
    {"key": "{aqi_desc}", "desc": "空气质量描述"},
    {"key": "{pm25}", "desc": "PM2.5"},
    {"key": "{pm10}", "desc": "PM10"},
    {"key": "{o3}", "desc": "臭氧"},
    {"key": "{so2}", "desc": "二氧化硫"},
    {"key": "{no2}", "desc": "二氧化氮"},
    {"key": "{co}", "desc": "一氧化碳"},
    {"key": "{pressure}", "desc": "气压(hPa)"},
    {"key": "{wind_direction}", "desc": "风向角度"},
    {"key": "{comfort_desc}", "desc": "舒适度"},
    {"key": "{car_washing}", "desc": "洗车指数"},
    {"key": "{dressing}", "desc": "穿衣指数"},
    {"key": "{cold_risk}", "desc": "感冒指数"},
    {"key": "{cloud_type}", "desc": "云型名称"},
    {"key": "{cloud_tip}", "desc": "拍摄建议"},
    {"key": "{data_source}", "desc": "数据来源"},
]


def _is_skycon_blocked(weather):
    """判断当前天气是否为不可见日落的状态（雾/霾/雨雪/沙尘）"""
    # 检查原始 skycon_raw（英文枚举值）
    raw = weather.get("skycon_raw", "")
    if raw and raw in BLOCKED_SKYCONS:
        return True
    # 检查翻译后的 skycon（中文文本）
    zh = weather.get("skycon", "")
    if zh and any(kw in zh for kw in BLOCKED_SKYCON_ZH):
        return True
    # 能见度极低也算（<2km 基本看不到日落轮廓）
    if weather.get("visibility", 10) < 2:
        return True
    return False


def classify_cloud_type(weather):
    """根据天气数据推断晚霞云层类型"""
    for key, info in SUNSET_CLOUD_TYPES.items():
        try:
            if info["condition"](weather):
                return {"key": key, "name": info["name"], "desc": info["desc"], "tip": info["tip"]}
        except Exception:
            pass
    return {"key": "default", "name": "普通晚霞", "desc": "一般云层条件", "tip": "出门走走，也许有惊喜"}


def format_push_message(weather, score_result, location_name, sunset_time, data_source, template=None):
    """使用模板生成推送消息，支持早间和傍晚两套模板"""
    if template is None:
        template = get_config("push_template", DEFAULT_PUSH_TEMPLATE)
    cloud = classify_cloud_type(weather)
    is_blocked = score_result.get("blocked", False)
    if is_blocked:
        rec_tag = "🚫今天看不到日落"
    elif score_result["recommend"]:
        rec_tag = "✅推荐拍摄"
    else:
        rec_tag = "⚠️条件一般"
    # 构建所有可用变量
    variables = {
        "location": location_name,
        "score": score_result["score"],
        "recommend_tag": rec_tag,
        "sunset_time": sunset_time,
        "sunrise": weather.get("sunrise", ""),
        "temperature": weather.get("temperature", "?"),
        "apparent_temperature": weather.get("apparent_temperature", "?"),
        "temp_high": weather.get("temp_high", "?"),
        "temp_low": weather.get("temp_low", "?"),
        "humidity": weather.get("humidity", "?"),
        "cloudrate": weather.get("cloudrate", "?"),
        "sunset_cloudrate": weather.get("sunset_cloudrate", "?"),
        "cloud_high": weather.get("cloud_high", "?"),
        "cloud_mid": weather.get("cloud_mid", "?"),
        "cloud_low": weather.get("cloud_low", "?"),
        "wind": weather.get("wind", "?"),
        "visibility": weather.get("visibility", "?"),
        "precip_probability": weather.get("precip_probability", "?"),
        "skycon": weather.get("skycon", ""),
        "forecast_keypoint": weather.get("forecast_keypoint", ""),
        "uv_desc": weather.get("uv_desc", ""),
        "aqi": weather.get("aqi", ""),
        "aqi_usa": weather.get("aqi_usa", ""),
        "aqi_desc": weather.get("aqi_desc", ""),
        "pm25": weather.get("pm25") or weather.get("pm2_5", ""),
        "pm10": weather.get("pm10", ""),
        "dust": weather.get("dust", ""),
        "o3": weather.get("o3") or weather.get("ozone", ""),
        "so2": weather.get("so2", ""),
        "no2": weather.get("no2", ""),
        "co": weather.get("co", ""),
        "us_aqi": weather.get("us_aqi", ""),
        "pressure": weather.get("pressure", ""),
        "wind_direction": weather.get("wind_direction", ""),
        "comfort_desc": weather.get("comfort_desc", ""),
        "car_washing": weather.get("car_washing", ""),
        "dressing": weather.get("dressing", ""),
        "cold_risk": weather.get("cold_risk", ""),
        "cloud_type": cloud["name"],
        "cloud_tip": cloud["tip"],
        "data_source": data_source,
    }
    try:
        text = template.format(**variables)
        return text
    except (KeyError, IndexError, ValueError) as e:
        log.warning(f"推送文案格式化失败: {e}，使用默认")
        return DEFAULT_PUSH_TEMPLATE.format(
            location=location_name, score=score_result["score"],
            recommend_tag=rec_tag, sunset_time=sunset_time,
            temperature=weather.get("temperature", "?"), humidity=weather.get("humidity", "?"),
            cloudrate=weather.get("cloudrate", "?"),
            cloud_high=weather.get("cloud_high", "?"), cloud_mid=weather.get("cloud_mid", "?"),
            cloud_low=weather.get("cloud_low", "?"),
            wind=weather.get("wind", "?"), visibility=weather.get("visibility", "?"),
            cloud_type=cloud["name"], cloud_tip=cloud["tip"], data_source=data_source,
        )

# ==================== 日志 ====================
def setup_logging():
    from logging.handlers import TimedRotatingFileHandler
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    log_file = LOGS_DIR / "bot.log"
    # 自动轮转：每天午夜切分，保留最近 30 天
    rotating_handler = TimedRotatingFileHandler(
        log_file, when="midnight", interval=1, backupCount=30, encoding="utf-8"
    )
    rotating_handler.suffix = "%Y%m%d"
    # 轮转命名：bot.log.20260804 → bot_20260804.log
    import re as _re
    def _namer(name):
        m = _re.match(r"^(.+)\.log\.(\d+)$", name)
        if m:
            base = m.group(1).replace(".", "_")
            return f"{base}_{m.group(2)}.log"
        return name
    rotating_handler.namer = _namer
    rotating_handler.setLevel(logging.INFO)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            rotating_handler,
            logging.StreamHandler(),
        ],
    )
    return logging.getLogger("sunset-bot")


log = setup_logging()


# ==================== 数据库 ====================
_db_local = threading.local()

def get_db():
    """获取数据库连接（线程本地复用，WAL 模式提升并发性能）"""
    conn = getattr(_db_local, 'conn', None)
    if conn is not None:
        try:
            conn.execute("SELECT 1")
            return conn
        except Exception:
            _db_local.conn = None
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH), timeout=DB_TIMEOUT)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=-4000")  # 4MB 缓存
    _db_local.conn = conn
    return conn


# ==================== 数据库版本迁移 ====================
# 每个迁移项: (目标版本, 描述, SQL 或函数)
# SQL 为字符串时直接 executescript；为 callable 时接收 conn 参数
DB_SCHEMA_VERSION = 1  # 当前最新 schema 版本

def _migration_v1(conn):
    """v1: 初始完整表结构"""
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS credentials (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL,
        updated_at INTEGER NOT NULL
    );
    CREATE TABLE IF NOT EXISTS locations (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        lng REAL NOT NULL,
        lat REAL NOT NULL,
        enabled INTEGER DEFAULT 1,
        created_at INTEGER NOT NULL
    );
    CREATE TABLE IF NOT EXISTS config (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL,
        updated_at INTEGER NOT NULL
    );
    CREATE TABLE IF NOT EXISTS push_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        location_id INTEGER,
        score INTEGER,
        pushed_at INTEGER NOT NULL,
        channel TEXT,
        success INTEGER DEFAULT 0,
        message TEXT
    );
    CREATE TABLE IF NOT EXISTS user (
        id INTEGER PRIMARY KEY,
        password_hash TEXT NOT NULL,
        created_at INTEGER NOT NULL
    );
    CREATE TABLE IF NOT EXISTS sessions (
        token TEXT PRIMARY KEY,
        created_at INTEGER NOT NULL,
        expires_at INTEGER NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_push_history_time ON push_history(pushed_at);
    CREATE INDEX IF NOT EXISTS idx_sessions_expires ON sessions(expires_at);
    CREATE TABLE IF NOT EXISTS weather_cache (
        location_id INTEGER PRIMARY KEY,
        data TEXT NOT NULL,
        fetched_at INTEGER NOT NULL,
        source TEXT DEFAULT 'api'
    );
    CREATE TABLE IF NOT EXISTS audit_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        action TEXT NOT NULL,
        target TEXT,
        detail TEXT,
        result TEXT DEFAULT 'success',
        created_at INTEGER NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_audit_log_time ON audit_log(created_at);
    CREATE TABLE IF NOT EXISTS scheduled_push (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        push_time TEXT NOT NULL,
        sunset_time TEXT NOT NULL,
        sunset_offset INTEGER NOT NULL,
        location_id INTEGER NOT NULL,
        created_at INTEGER NOT NULL
    );
    CREATE TABLE IF NOT EXISTS sunset_photos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        location_id INTEGER,
        predicted_score REAL,
        ai_score REAL,
        ai_cloud_coverage REAL,
        ai_color_saturation REAL,
        ai_description TEXT,
        photo_path TEXT,
        calibration_offset REAL,
        created_at INTEGER NOT NULL
    );
    """)

# 迁移注册表：按版本号升序排列，新增迁移追加到末尾
MIGRATIONS = [
    (1, "初始完整表结构", _migration_v1),
    # 未来新增迁移示例:
    # (2, "添加 xxx 表/字段", "ALTER TABLE ... ADD COLUMN ...;"),
]


def _get_schema_version(conn):
    """获取当前 schema 版本，无版本表则返回 0"""
    try:
        row = conn.execute("SELECT version FROM schema_version LIMIT 1").fetchone()
        return row[0] if row else 0
    except Exception:
        return 0


def _set_schema_version(conn, version):
    """更新 schema 版本"""
    conn.execute("CREATE TABLE IF NOT EXISTS schema_version (version INTEGER NOT NULL)")
    conn.execute("DELETE FROM schema_version")
    conn.execute("INSERT INTO schema_version (version) VALUES (?)", (version,))


def init_db():
    """初始化数据库：按版本顺序执行待处理的迁移"""
    conn = get_db()
    current = _get_schema_version(conn)
    if current >= DB_SCHEMA_VERSION:
        log.info(f"数据库 schema 已是最新版本 (v{current})")
        conn.close()
        return
    # 依次执行待处理的迁移
    for target_ver, desc, migration in MIGRATIONS:
        if target_ver <= current:
            continue
        log.info(f"执行数据库迁移 v{target_ver}: {desc}")
        try:
            if callable(migration):
                migration(conn)
            else:
                conn.executescript(migration)
            _set_schema_version(conn, target_ver)
            conn.commit()
        except Exception as e:
            log.error(f"数据库迁移 v{target_ver} 失败: {e}")
            conn.rollback()
            conn.close()
            raise
    conn.close()
    log.info(f"数据库初始化完成 (v{DB_SCHEMA_VERSION})")


# ==================== 天气缓存 ===================
def get_weather_cache(location_id):
    """获取缓存的天气数据，返回 (data_dict, fetched_at) 或 (None, None)"""
    conn = get_db()
    row = conn.execute(
        "SELECT data, fetched_at, source FROM weather_cache WHERE location_id=?",
        (location_id,)
    ).fetchone()
    conn.close()
    if row:
        try:
            return json.loads(row["data"]), row["fetched_at"], row["source"]
        except Exception:
            pass
    return None, None, None


def set_weather_cache(location_id, data, source="api"):
    """写入天气缓存"""
    conn = get_db()
    conn.execute(
        "INSERT OR REPLACE INTO weather_cache (location_id, data, fetched_at, source) VALUES (?, ?, ?, ?)",
        (location_id, json.dumps(data, ensure_ascii=False), int(time.time()), source)
    )
    conn.commit()
    conn.close()
    log.info(f"天气缓存已更新: location_id={location_id}")


def get_all_weather_cache():
    """获取所有地点的缓存数据"""
    conn = get_db()
    rows = conn.execute("SELECT * FROM weather_cache").fetchall()
    conn.close()
    result = {}
    for row in rows:
        try:
            result[row["location_id"]] = {
                "data": json.loads(row["data"]),
                "fetched_at": row["fetched_at"],
                "source": row["source"],
            }
        except Exception:
            pass
    return result


def clear_weather_cache(location_id=None):
    """清除天气缓存（指定地点或全部）"""
    conn = get_db()
    if location_id:
        conn.execute("DELETE FROM weather_cache WHERE location_id=?", (location_id,))
        log.info(f"已清除地点 {location_id} 的天气缓存")
    else:
        count = conn.execute("SELECT COUNT(*) FROM weather_cache").fetchone()[0]
        conn.execute("DELETE FROM weather_cache")
        log.info(f"已清除全部天气缓存 ({count} 条)")
    conn.commit()
    conn.close()


# ==================== 凭证管理 ====================
def get_credential(key, default=""):
    conn = get_db()
    row = conn.execute("SELECT value FROM credentials WHERE key=?", (key,)).fetchone()
    conn.close()
    return row["value"] if row else default


def set_credential(key, value):
    conn = get_db()
    conn.execute(
        "INSERT OR REPLACE INTO credentials (key, value, updated_at) VALUES (?, ?, ?)",
        (key, value, int(time.time()))
    )
    conn.commit()
    conn.close()
    log.info(f"凭证更新: {key}")


def get_all_credentials():
    conn = get_db()
    rows = conn.execute("SELECT key, value FROM credentials").fetchall()
    conn.close()
    return {r["key"]: r["value"] for r in rows}


def delete_credential(key):
    conn = get_db()
    conn.execute("DELETE FROM credentials WHERE key=?", (key,))
    conn.commit()
    conn.close()
    log.info(f"凭证删除: {key}")


# ==================== 地点管理 ====================
def get_locations(enabled_only=False):
    conn = get_db()
    if enabled_only:
        rows = conn.execute("SELECT * FROM locations WHERE enabled=1 ORDER BY id").fetchall()
    else:
        rows = conn.execute("SELECT * FROM locations ORDER BY id").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def add_location(name, lng, lat):
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO locations (name, lng, lat, enabled, created_at) VALUES (?, ?, ?, 1, ?)",
        (name, lng, lat, int(time.time()))
    )
    conn.commit()
    new_id = cur.lastrowid
    conn.close()
    log.info(f"添加地点: {name} ({lng},{lat}) id={new_id}")
    return new_id


def update_location(loc_id, name=None, lng=None, lat=None, enabled=None):
    # 白名单列名映射（防止 SQL 注入）
    ALLOWED_COLUMNS = {"name": "name", "lng": "lng", "lat": "lat", "enabled": "enabled"}
    updates = {}
    if name is not None:
        updates["name"] = name
    if lng is not None:
        updates["lng"] = lng
    if lat is not None:
        updates["lat"] = lat
    if enabled is not None:
        updates["enabled"] = 1 if enabled else 0
    if not updates:
        return False
    # 构建参数化 SQL（列名来自白名单，值全部参数化）
    set_clauses = [f"{ALLOWED_COLUMNS[col]}=?" for col in updates if col in ALLOWED_COLUMNS]
    params = [updates[col] for col in updates if col in ALLOWED_COLUMNS]
    if not set_clauses:
        return False
    params.append(loc_id)
    sql = "UPDATE locations SET " + ", ".join(set_clauses) + " WHERE id=?"
    conn = get_db()
    conn.execute(sql, params)
    conn.commit()
    conn.close()
    log.info(f"更新地点 id={loc_id}")
    return True


def delete_location(loc_id):
    conn = get_db()
    conn.execute("DELETE FROM locations WHERE id=?", (loc_id,))
    conn.commit()
    conn.close()
    log.info(f"删除地点 id={loc_id}")


# ==================== 配置管理 ====================
def get_config(key, default=None):
    conn = get_db()
    row = conn.execute("SELECT value FROM config WHERE key=?", (key,)).fetchone()
    conn.close()
    if not row:
        return default
    try:
        return json.loads(row["value"])
    except Exception:
        return default


def set_config(key, value):
    conn = get_db()
    conn.execute(
        "INSERT OR REPLACE INTO config (key, value, updated_at) VALUES (?, ?, ?)",
        (key, json.dumps(value, ensure_ascii=False), int(time.time()))
    )
    conn.commit()
    conn.close()


def get_all_config():
    conn = get_db()
    rows = conn.execute("SELECT key, value FROM config").fetchall()
    conn.close()
    return {r["key"]: json.loads(r["value"]) for r in rows}


# ==================== 晚霞评分（从 scoring.py 导入） ===================
from scoring import score_sunset  # noqa: F401


# ==================== AI 晚霞照片分析 ===================
_AI_PROVIDERS = {
    "gemini": {
        "name": "Google Gemini",
        "key_name": "gemini_key",
        "analyze_func": "_analyze_gemini",
    },
    "zhipu": {
        "name": "智谱AI",
        "key_name": "zhipu_key",
        "analyze_func": "_analyze_zhipu",
    },
}

def analyze_sunset_photo(photo_base64: str, rounds: int = 3) -> dict:
    """
    调用视觉模型分析晚霞照片，多次分析取平均值
    支持 Gemini（优先）和智谱AI（备用）
    rounds: 分析次数，默认3次
    """
    # 确定使用的服务商（优先 Gemini）
    provider = get_config("ai_provider", "gemini") or "gemini"
    cfg = _AI_PROVIDERS.get(provider)
    if not cfg:
        cfg = _AI_PROVIDERS["gemini"]
        provider = "gemini"

    api_key = get_credential(cfg["key_name"], "")
    if not api_key:
        # 尝试其他服务商
        for p, c in _AI_PROVIDERS.items():
            api_key = get_credential(c["key_name"], "")
            if api_key:
                log.info(f"[AI分析] {cfg['name']} 未配置，自动切换到 {c['name']}")
                provider, cfg = p, c
                break
        if not api_key:
            log.warning("[AI分析] 未配置任何 AI Key，跳过")
            return None

    prompt = """请分析这张晚霞/日落照片，返回 JSON 格式结果：
{
  "ai_score": 晚霞综合质量评分（0-100，越高越好），
  "cloud_coverage": 天空中云的覆盖比例（0-100%），
  "color_saturation": 色彩饱和度/丰富程度（0-100，红色/橙色/紫色越丰富越高），
  "description": "简短描述晚霞状况（20字以内）"
}
只返回 JSON，不要其他文字。"""

    # 获取分析函数
    analyze_func = globals().get(cfg["analyze_func"])
    if not analyze_func:
        log.error(f"[AI分析] 未找到分析函数: {cfg['analyze_func']}")
        return None

    # 多次分析取平均值
    results = []
    for i in range(rounds):
        result = analyze_func(photo_base64, prompt, api_key)
        if result:
            results.append(result)
            log.info(f"[AI分析-{cfg['name']}] 第{i+1}/{rounds}次: 评分={result.get('ai_score')} 云量={result.get('cloud_coverage')}%")

    if not results:
        return None

    # 计算平均值
    avg_score = sum(r["ai_score"] for r in results) / len(results)
    avg_cloud = sum(r["cloud_coverage"] for r in results) / len(results)
    avg_color = sum(r["color_saturation"] for r in results) / len(results)

    # 取中间值的描述（按ai_score排序后取中间）
    results.sort(key=lambda x: x["ai_score"])
    mid_desc = results[len(results) // 2].get("description", "")

    final = {
        "ai_score": round(avg_score, 1),
        "cloud_coverage": round(avg_cloud, 1),
        "color_saturation": round(avg_color, 1),
        "description": mid_desc,
    }
    log.info(f"[AI分析] {len(results)}次平均结果: 评分={final['ai_score']} 云量={final['cloud_coverage']}% 色彩={final['color_saturation']}")
    return final


def _analyze_gemini(photo_base64: str, prompt: str, api_key: str) -> dict:
    """调用 Google Gemini 分析"""
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={api_key}"
    payload = {
        "contents": [{
            "parts": [
                {"text": prompt},
                {"inline_data": {"mime_type": "image/jpeg", "data": photo_base64}},
            ]
        }],
        "generationConfig": {
            "temperature": 0.1,
            "maxOutputTokens": 300,
        },
    }
    # 获取代理配置
    proxy = get_config("https_proxy", "")
    proxies = {"http": proxy, "https": proxy} if proxy else None
    try:
        resp = get_http_session().post(url, json=payload, timeout=AI_TIMEOUT, proxies=proxies)
        if resp.status_code != 200:
            log.warning(f"[AI分析-Gemini] API 返回 {resp.status_code}: {resp.text[:200]}")
            return None
        data = resp.json()
        content = data["candidates"][0]["content"]["parts"][0]["text"]
        log.info(f"[AI分析-Gemini] 原始响应: {content[:200]}")
        return _parse_ai_response(content, "Gemini")
    except Exception as e:
        log.warning(f"[AI分析-Gemini] 异常: {e}")
        return None


def _analyze_zhipu(photo_base64: str, prompt: str, api_key: str) -> dict:
    """调用智谱AI GLM-4V分析"""
    url = "https://open.bigmodel.cn/api/paas/v4/chat/completions"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": "glm-4v-flash",
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{photo_base64}"}},
                    {"type": "text", "text": prompt},
                ],
            }
        ],
        "max_tokens": 300,
        "temperature": 0.1,
    }
    try:
        resp = get_http_session().post(url, json=payload, headers=headers, timeout=AI_TIMEOUT)
        if resp.status_code != 200:
            log.warning(f"[AI分析-智谱] API 返回 {resp.status_code}: {resp.text[:200]}")
            return None
        data = resp.json()
        content = data["choices"][0]["message"]["content"]
        log.info(f"[AI分析-智谱] 原始响应: {content[:200]}")
        return _parse_ai_response(content, "智谱")
    except Exception as e:
        log.warning(f"[AI分析-智谱] 异常: {e}")
        return None


def get_push_score_by_date(location_id, photo_date):
    """根据地点和日期查询当天推送评分"""
    try:
        from datetime import datetime
        dt = datetime.strptime(photo_date, "%Y-%m-%d")
        start_ts = int(dt.timestamp())
        end_ts = start_ts + 86400  # +1天
        conn = get_db()
        row = conn.execute(
            "SELECT score FROM push_history WHERE location_id=? AND pushed_at>=? AND pushed_at<? ORDER BY pushed_at DESC LIMIT 1",
            (location_id, start_ts, end_ts)
        ).fetchone()
        conn.close()
        if row:
            return row[0]
    except Exception as e:
        log.debug(f"[AI校准] 查询推送历史失败: {e}")
    return None


def _parse_ai_response(content: str, source: str) -> dict:
    """解析 AI 响应 JSON"""
    json_match = re.search(r'\{[^}]+\}', content, re.DOTALL)
    if json_match:
        result = json.loads(json_match.group())
        for key in ["ai_score", "cloud_coverage", "color_saturation"]:
            val = result.get(key, 0)
            result[key] = max(0, min(100, float(val)))
        result.setdefault("description", "")
        log.info(f"[AI分析-{source}] 晚霞评分={result['ai_score']} 云量={result['cloud_coverage']}% 色彩={result['color_saturation']}")
        return result
    else:
        log.warning(f"[AI分析-{source}] 无法从响应中提取 JSON")
        return None


def save_sunset_photo(location_id, predicted_score, ai_result, photo_path="", photo_date=None):
    """保存 AI 分析结果并计算校准偏移量"""
    if not ai_result:
        return
    # 计算偏移量：AI评分 - 预测评分
    ai_score = ai_result.get("ai_score", 0)
    offset = round(ai_score - predicted_score, 1) if predicted_score else 0

    # 计算时间戳：如果指定了日期/时间则用指定值，否则用当前时间
    if photo_date:
        from datetime import datetime
        try:
            # 尝试解析完整时间 "2026-07-18 19:30" 或仅日期 "2026-07-18"
            if " " in photo_date:
                dt = datetime.strptime(photo_date, "%Y-%m-%d %H:%M")
            else:
                dt = datetime.strptime(photo_date, "%Y-%m-%d")
            created_at = int(dt.timestamp())
        except ValueError:
            created_at = int(time.time())
    else:
        created_at = int(time.time())

    conn = get_db()
    conn.execute(
        """INSERT INTO sunset_photos
           (location_id, predicted_score, ai_score, ai_cloud_coverage, ai_color_saturation,
            ai_description, photo_path, calibration_offset, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (location_id, predicted_score, ai_score,
         ai_result.get("cloud_coverage", 0), ai_result.get("color_saturation", 0),
         ai_result.get("description", ""), photo_path, offset, created_at)
    )
    conn.commit()
    conn.close()
    log.info(f"[AI校准] 保存分析结果: 预测={predicted_score} AI={ai_score} 偏移={offset}")
    return offset


def get_calibration_offset(location_id=None, max_records=10):
    """获取近期 AI 校准偏移量的中位数"""
    try:
        conn = get_db()
        if location_id:
            rows = conn.execute(
                "SELECT calibration_offset FROM sunset_photos WHERE location_id=? ORDER BY created_at DESC LIMIT ?",
                (location_id, max_records)
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT calibration_offset FROM sunset_photos ORDER BY created_at DESC LIMIT ?",
                (max_records,)
            ).fetchall()
        conn.close()

        if not rows:
            return 0

        offsets = sorted([r["calibration_offset"] for r in rows])
        median = offsets[len(offsets) // 2]
        return round(median, 1)
    except Exception as e:
        log.debug(f"[AI校准] 获取偏移量失败: {e}")
        return 0


# ==================== 天气 API 调用（自动降级）==================
_token_cache = {}  # {provider_name: token} 内存缓存，避免每次查库

def _get_provider_token(provider):
    """获取指定 provider 的 token（带内存缓存）"""
    if provider.name in _token_cache:
        return _token_cache[provider.name]
    token_key = f"{provider.name}_token"
    token = get_credential(token_key, "")
    # 兼容旧配置
    if not token and provider.name == "caiyun":
        token = get_credential("caiyun_token", "")
    _token_cache[provider.name] = token
    return token

def invalidate_token_cache():
    """凭证更新时清除 token 缓存"""
    _token_cache.clear()


def fetch_weather(lng, lat, timeout=10, no_fallback=False):
    """
    通过天气 API 拉取数据，支持自动降级
    主 provider 失败时自动尝试 PROVIDER_FALLBACK_ORDER 中的下一个
    - no_fallback=True: 仅尝试主 provider，失败直接返回（手动拉取用）
    返回: (weather, error)
    """
    primary = get_weather_provider()
    errors = []

    # 构建尝试顺序：主 provider 优先，然后是降级列表
    if no_fallback:
        try_order = [primary.name]
    else:
        try_order = [primary.name] + [p for p in PROVIDER_FALLBACK_ORDER if p != primary.name]

    for name in try_order:
        provider = WEATHER_PROVIDERS.get(name)
        if not provider:
            continue
        token = _get_provider_token(provider)
        # 免费源（Open-Meteo 系列）不需要 token
        if not token and provider.name not in FREE_PROVIDER_NAMES:
            continue
        log.info(f"尝试天气 API: {provider.display_name} ({provider.name})")
        weather, err = provider.fetch(lng, lat, token, timeout=timeout)
        if weather:
            if name != primary.name:
                log.warning(f"主 API ({primary.display_name}) 不可用，已降级到 {provider.display_name}")
            log.info(f"天气数据({provider.display_name}): 云量={weather['cloudrate']}% 湿度={weather['humidity']}% 风速={weather['wind']}m/s 能见度={weather['visibility']}km 温度={weather['temperature']}°C")
            # 记录实际使用的 provider
            weather["_provider"] = provider.name
            return weather, None
        errors.append(f"{provider.display_name}: {err}")
        log.warning(f"{provider.display_name} 失败: {err}")

    return None, f"所有天气 API 均失败: {'; '.join(errors)}"


# 天气缓存硬过期时间（秒）：超过此时间不再返回缓存，强制刷新
WEATHER_CACHE_TTL = 3600  # 1 小时

def fetch_weather_cached(location_id, lng, lat, force_refresh=False, timeout=10, no_fallback=False):
    """
    获取天气数据（带缓存 + TTL 自动过期）
    - force_refresh=False: 优先返回缓存（TTL 内），过期才调 API
    - force_refresh=True: 强制调 API 并更新缓存
    - no_fallback=True: 仅尝试主 provider，失败不回退旧缓存（手动拉取用）
    返回 (weather, source, error)
      source: "cache" 或具体 API 名称（如"彩云天气"）
    """
    if not force_refresh:
        cached, fetched_at, src = get_weather_cache(location_id)
        if cached:
            cache_age = time.time() - fetched_at if fetched_at else 0
            if cache_age <= WEATHER_CACHE_TTL:
                log.info(f"[loc={location_id}] 使用缓存数据 (age={int(cache_age)}s, TTL={WEATHER_CACHE_TTL}s)")
                cached["_fetched_at"] = fetched_at  # 注入时间戳供评分衰减
                return cached, src or "cache", None
            else:
                log.info(f"[loc={location_id}] 缓存已过期 (age={int(cache_age)}s > TTL={WEATHER_CACHE_TTL}s)，刷新")

    # 调 API
    weather, err = fetch_weather(lng, lat, timeout=timeout, no_fallback=no_fallback)
    if err:
        if no_fallback:
            # 手动拉取：失败直接返回，不降级
            return None, None, err
        # 定时任务：API 失败，尝试回退到旧缓存
        cached, fetched_at, src = get_weather_cache(location_id)
        if cached:
            log.warning(f"[loc={location_id}] API 失败 ({err})，使用旧缓存")
            cached["_fetched_at"] = fetched_at
            return cached, "cache_old", err
        return None, None, err

    # 成功则更新缓存，记录具体 API 来源
    weather["_fetched_at"] = time.time()  # 注入新鲜数据时间戳
    provider_name = weather.get("_provider", "")
    provider_obj = WEATHER_PROVIDERS.get(provider_name)
    source_label = provider_obj.display_name if provider_obj else "api"
    set_weather_cache(location_id, weather, source=source_label)
    return weather, source_label, None


def refresh_all_weather_cache():
    """手动刷新所有启用地点的天气缓存（先清旧缓存再拉取新数据，支持交叉验证）"""
    locations = get_locations(enabled_only=True)
    results = {}
    use_multi = get_config("cross_validation", False)
    for loc in locations:
        # 先清除旧缓存，确保新数据写入时旧数据已删除
        clear_weather_cache(loc["id"])
        if use_multi:
            weather, err = fetch_weather_multi(loc["lng"], loc["lat"])
            source = weather.get("_source_label", "multi") if weather else "multi(error)"
            if weather:
                set_weather_cache(loc["id"], weather, source=source)
        else:
            weather, source, err = fetch_weather_cached(loc["id"], loc["lng"], loc["lat"], force_refresh=True, no_fallback=True)
        results[loc["id"]] = {
            "name": loc["name"],
            "success": weather is not None,
            "source": source,
            "error": err,
        }
        if weather:
            log.info(f"[{loc['name']}] 天气缓存已刷新")
        else:
            log.warning(f"[{loc['name']}] 刷新失败: {err}")
    return results


# ==================== 日落时间计算 ===================
def calc_sunset_time(lat, lng, date=None, timezone_offset=None):
    """
    天文公式计算日落时间（NOAA 简化版）
    返回 datetime 对象（当天日落时刻，本地时间近似）
    精度约 ±2 分钟
    
    timezone_offset: 时区偏移（小时），优先使用 API 提供的值
                     如果未提供，则从经度自动推算
    """
    import math
    if date is None:
        date = datetime.now().date()
    
    # 一年中的第几天
    day_of_year = (date - date.replace(month=1, day=1)).days + 1
    
    # 太阳赤纬（NOAA 近似公式）
    gamma = 2 * math.pi / 365 * (day_of_year - 1)
    declination = (0.006918 - 0.399912 * math.cos(gamma) + 0.070257 * math.sin(gamma)
                   - 0.006758 * math.cos(2 * gamma) + 0.000907 * math.sin(2 * gamma)
                   - 0.002697 * math.cos(3 * gamma) + 0.00148 * math.sin(3 * gamma))
    
    # 时间方程（分钟）
    eqtime = (229.18 * (0.000075 + 0.001868 * math.cos(gamma) - 0.032077 * math.sin(gamma)
              - 0.014615 * math.cos(2 * gamma) - 0.040849 * math.sin(2 * gamma)))
    
    # 时角（日落时太阳高度角 = -0.833°，考虑大气折射）
    lat_rad = math.radians(lat)
    zenith = math.radians(90.833)  # 日落天顶角
    
    cos_hour = ((math.cos(zenith) / (math.cos(lat_rad) * math.cos(declination)))
                - math.tan(lat_rad) * math.tan(declination))
    # 限制范围避免 acos 错误
    cos_hour = max(-1.0, min(1.0, cos_hour))
    hour_angle = math.degrees(math.acos(cos_hour))
    
    # 日落时间（分钟，从午夜开始，本地标准时间）
    # NOAA 公式: sunset = 720 - 4*(longitude - hour_angle) - eqtime + timezone*60
    if timezone_offset is None:
        timezone_offset = round(lng / 15)  # fallback: 从经度推算时区
    sunset_minutes = 720 - 4 * (lng - hour_angle) - eqtime + timezone_offset * 60
    
    # 转为小时和分钟
    h = int(sunset_minutes // 60)
    m = int(sunset_minutes % 60)
    h = max(0, min(23, h))
    m = max(0, min(59, m))
    
    log.info(f"日落计算: {date} lat={lat} lng={lng} → {h:02d}:{m:02d} (NOAA)")
    return datetime(date.year, date.month, date.day, h, m)


def get_sunset_time_for_location(lng, lat, weather=None):
    """
    获取当天日落时间
    优先使用天气 API 返回的时区偏移，fallback 到经度推算
    返回 (datetime, source_str)
    """
    try:
        # 优先从天气数据中获取 API 提供的时区偏移
        tz_offset = None
        if weather and "timezone_offset" in weather:
            tz_offset = weather["timezone_offset"]
            log.info(f"使用 API 时区偏移: UTC+{tz_offset}")
        sunset_dt = calc_sunset_time(lat, lng, timezone_offset=tz_offset)
        source = "api_tz" if tz_offset is not None else "calc"
        return sunset_dt, source
    except Exception as e:
        log.warning(f"日落时间计算失败: {e}")
        # 默认 18:30
        now = datetime.now()
        return datetime(now.year, now.month, now.day, 18, 30), "default"


def _calc_sunrise_time(lat, lng, date=None, timezone_offset=None):
    """计算当天日出时间（NOAA 公式），返回 datetime
    timezone_offset: 时区偏移（小时），优先使用 API 提供的值
    """
    import math
    if date is None:
        date = datetime.now().date()
    day_of_year = (date - date.replace(month=1, day=1)).days + 1
    gamma = 2 * math.pi / 365 * (day_of_year - 1)
    declination = (0.006918 - 0.399912 * math.cos(gamma) + 0.070257 * math.sin(gamma)
                   - 0.006758 * math.cos(2 * gamma) + 0.000907 * math.sin(2 * gamma)
                   - 0.002697 * math.cos(3 * gamma) + 0.00148 * math.sin(3 * gamma))
    eqtime = (229.18 * (0.000075 + 0.001868 * math.cos(gamma) - 0.032077 * math.sin(gamma)
              - 0.014615 * math.cos(2 * gamma) - 0.040849 * math.sin(2 * gamma)))
    lat_rad = math.radians(lat)
    zenith = math.radians(90.833)
    cos_hour = ((math.cos(zenith) / (math.cos(lat_rad) * math.cos(declination)))
                - math.tan(lat_rad) * math.tan(declination))
    cos_hour = max(-1.0, min(1.0, cos_hour))
    hour_angle = math.degrees(math.acos(cos_hour))
    # 日出公式: sunrise = 720 - 4*(longitude + hour_angle) - eqtime + timezone*60
    if timezone_offset is None:
        timezone_offset = round(lng / 15)  # fallback: 从经度推算时区
    sunrise_minutes = 720 - 4 * (lng + hour_angle) - eqtime + timezone_offset * 60
    h = max(0, min(23, int(sunrise_minutes // 60)))
    m = max(0, min(59, int(sunrise_minutes % 60)))
    return datetime(date.year, date.month, date.day, h, m)


# ==================== 推送通道（从 pusher.py 导入） ===================
from pusher import (  # noqa: F401
    push_wechat, push_dingtalk, push_feishu,
    push_serverchan, push_pushplus,
    push_wechat_markdown, push_feishu_rich,
    push_message, test_webhook,
)


# ==================== 推送历史 ===================
def record_push(location_id, score, channel, success, message=""):
    conn = get_db()
    conn.execute(
        "INSERT INTO push_history (location_id, score, pushed_at, channel, success, message) VALUES (?, ?, ?, ?, ?, ?)",
        (location_id, score, int(time.time()), channel, 1 if success else 0, message)
    )
    conn.commit()
    conn.close()


def get_push_history(limit=DEFAULT_PUSH_HISTORY_LIMIT):
    conn = get_db()
    rows = conn.execute(
        """SELECT h.*, COALESCE(l.name, '未知地点') as location_name
           FROM push_history h
           LEFT JOIN locations l ON h.location_id = l.id
           ORDER BY h.pushed_at DESC LIMIT ?""",
        (limit,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ==================== 操作审计日志 ===================
def audit(action, target=None, detail=None, result="success"):
    """记录操作审计日志"""
    conn = get_db()
    conn.execute(
        "INSERT INTO audit_log (action, target, detail, result, created_at) VALUES (?, ?, ?, ?, ?)",
        (action, target, detail, result, int(time.time()))
    )
    # 自动清理：每 N 条清理一次，保留最近 M 条
    count = conn.execute("SELECT COUNT(*) FROM audit_log").fetchone()[0]
    if count % AUDIT_LOG_AUTO_CLEAN_THRESHOLD == 0:
        conn.execute(f"DELETE FROM audit_log WHERE id NOT IN (SELECT id FROM audit_log ORDER BY created_at DESC LIMIT {AUDIT_LOG_MAX_COUNT})")
    conn.commit()
    conn.close()


def get_audit_logs(limit=DEFAULT_AUDIT_LOG_LIMIT):
    """获取审计日志"""
    conn = get_db()
    rows = conn.execute(
        "SELECT * FROM audit_log ORDER BY created_at DESC LIMIT ?", (limit,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ==================== 自动清理（日志 + 数据库） ===================
LOG_RETENTION_DAYS = 30          # 日志文件保留天数
PUSH_HISTORY_MAX_COUNT = 2000    # 推送历史最大保留条数

def cleanup_old_logs():
    """清理超过 LOG_RETENTION_DAYS 天的日志文件"""
    try:
        cutoff = time.time() - LOG_RETENTION_DAYS * 86400
        deleted = 0
        for f in LOGS_DIR.iterdir():
            if f.is_file() and f.name.startswith("bot_") and f.name.endswith(".log"):
                if f.stat().st_mtime < cutoff:
                    f.unlink()
                    deleted += 1
        if deleted:
            log.info(f"[自动清理] 删除 {deleted} 个过期日志文件 (>{LOG_RETENTION_DAYS}天)")
        return deleted
    except Exception as e:
        log.warning(f"[自动清理] 日志清理失败: {e}")
        return 0


def cleanup_push_history():
    """清理推送历史，只保留最近 PUSH_HISTORY_MAX_COUNT 条"""
    try:
        conn = get_db()
        count = conn.execute("SELECT COUNT(*) FROM push_history").fetchone()[0]
        if count > PUSH_HISTORY_MAX_COUNT:
            conn.execute(
                "DELETE FROM push_history WHERE id NOT IN "
                "(SELECT id FROM push_history ORDER BY pushed_at DESC LIMIT ?)",
                (PUSH_HISTORY_MAX_COUNT,)
            )
            conn.commit()
            deleted = count - PUSH_HISTORY_MAX_COUNT
            log.info(f"[自动清理] 清理推送历史: 删除 {deleted} 条 (保留 {PUSH_HISTORY_MAX_COUNT})")
            conn.close()
            return deleted
        conn.close()
        return 0
    except Exception as e:
        log.warning(f"[自动清理] 推送历史清理失败: {e}")
        return 0


def run_maintenance():
    """每日维护任务：清理过期日志 + 数据库瘦身"""
    log.info("[维护] 开始每日清理...")
    logs_deleted = cleanup_old_logs()
    pushes_deleted = cleanup_push_history()
    # audit_log 已有写入时自动清理（见 audit()），无需额外处理
    total = logs_deleted + pushes_deleted
    if total:
        log.info(f"[维护] 清理完成: {logs_deleted} 个日志 + {pushes_deleted} 条推送记录")
    else:
        log.info("[维护] 清理完成，无需清理")


# ==================== GitHub 版本更新检查 ===================
_update_cache = {"result": None, "checked_at": 0}
UPDATE_CACHE_TTL = 3600  # 缓存 1 小时，避免频繁请求 GitHub API


def check_github_update(force=False):
    """
    检查 GitHub 是否有新版本。
    返回: {"current": str, "latest": str|None, "has_update": bool, "release_url": str, "error": str|None}
    需要先在配置中设置 github_repo（如 "owner/repo"）。
    """
    import time as _t
    now = _t.time()
    # 缓存未过期且非强制，直接返回缓存
    if not force and _update_cache["result"] and (now - _update_cache["checked_at"]) < UPDATE_CACHE_TTL:
        return _update_cache["result"]

    repo = get_config("github_repo", "")
    result = {
        "current": APP_VERSION,
        "latest": None,
        "has_update": False,
        "release_url": "",
        "published_at": "",
        "error": None,
    }

    if not repo:
        result["error"] = "未配置 GitHub 仓库地址"
        _update_cache["result"] = result
        _update_cache["checked_at"] = now
        return result

    try:
        url = f"https://api.github.com/repos/{repo}/releases/latest"
        resp = get_http_session().get(url, timeout=API_TIMEOUT)
        if resp.status_code == 404:
            result["error"] = "仓库无 Release（可能尚未发布）"
        elif resp.status_code != 200:
            result["error"] = f"GitHub API 返回 {resp.status_code}"
        else:
            data = resp.json()
            tag = data.get("tag_name", "")
            # 去掉 v 前缀比较
            latest_ver = tag.lstrip("vV")
            current_ver = APP_VERSION.lstrip("vV")
            result["latest"] = latest_ver
            result["release_url"] = data.get("html_url", "")
            result["published_at"] = data.get("published_at", "")
            # 简单版本比较（按 . 分割逐段对比）
            try:
                latest_parts = [int(x) for x in latest_ver.split(".")]
                current_parts = [int(x) for x in current_ver.split(".")]
                result["has_update"] = latest_parts > current_parts
            except ValueError:
                result["has_update"] = latest_ver != current_ver
            log.info(f"[更新] 当前 v{APP_VERSION}，最新 v{latest_ver}，{'有更新' if result['has_update'] else '已是最新'}")
    except Exception as e:
        result["error"] = f"检查失败: {e}"
        log.warning(f"[更新] GitHub 检查异常: {e}")

    _update_cache["result"] = result
    _update_cache["checked_at"] = now
    return result


def download_github_release():
    """
    从 GitHub 下载最新 Release 的源码包，返回 {filename: bytes}。
    仅保留白名单内的文件。失败返回 (None, error_msg)。
    """
    import io, zipfile
    repo = get_config("github_repo", "")
    if not repo:
        return None, "未配置 GitHub 仓库地址"

    try:
        # 获取最新 release 信息
        url = f"https://api.github.com/repos/{repo}/releases/latest"
        resp = get_http_session().get(url, timeout=API_TIMEOUT)
        if resp.status_code != 200:
            return None, f"GitHub API 返回 {resp.status_code}"
        data = resp.json()
        tag = data.get("tag_name", "")
        if not tag:
            return None, "Release 无 tag_name"

        # 下载源码 zip
        zip_url = f"https://api.github.com/repos/{repo}/zipball/{tag}"
        log.info(f"[更新] 下载 GitHub Release: {tag}")
        resp = get_http_session().get(zip_url, timeout=60)
        if resp.status_code != 200:
            return None, f"下载源码失败: HTTP {resp.status_code}"

        # 解压并提取白名单文件
        zf = zipfile.ZipFile(io.BytesIO(resp.content))
        files = {}
        # zip 内第一层目录名是 repo-commitHash，需要跳过
        for name in zf.namelist():
            # 去掉第一层目录前缀
            parts = name.split("/", 1)
            if len(parts) < 2:
                continue
            rel_path = parts[1]
            if not rel_path or rel_path.endswith("/"):
                continue
            # 只保留白名单文件
            if rel_path in UPGRADE_ALLOWED_FILES:
                files[rel_path] = zf.read(name)
        zf.close()

        if not files:
            return None, "源码包中无匹配的可更新文件"

        log.info(f"[更新] 从 GitHub 提取 {len(files)} 个文件: {list(files.keys())}")
        return files, None

    except Exception as e:
        log.warning(f"[更新] 下载 GitHub Release 异常: {e}")
        return None, f"下载异常: {e}"


# ==================== 预约推送持久化 ===================
def save_scheduled_push(push_time, sunset_time, sunset_offset, location_id):
    """保存预约任务到数据库"""
    conn = get_db()
    cur = conn.execute(
        "INSERT INTO scheduled_push (push_time, sunset_time, sunset_offset, location_id, created_at) VALUES (?, ?, ?, ?, ?)",
        (push_time, sunset_time, sunset_offset, location_id, int(time.time()))
    )
    conn.commit()
    task_id = cur.lastrowid
    conn.close()
    return task_id


def get_scheduled_pushes():
    """获取所有待执行的预约任务"""
    conn = get_db()
    rows = conn.execute("SELECT * FROM scheduled_push ORDER BY push_time").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def delete_scheduled_push(task_id):
    """删除预约任务"""
    conn = get_db()
    conn.execute("DELETE FROM scheduled_push WHERE id=?", (task_id,))
    conn.commit()
    conn.close()


# ==================== 静默失败告警 ===================
_consecutive_failures = {}  # {location_id: count}
FAILURE_ALERT_THRESHOLD = 3  # 连续失败N次触发告警


def check_silent_failure(location_id, success, error_msg=""):
    """检查连续失败，超过阈值自动发送告警"""
    global _consecutive_failures
    if success:
        _consecutive_failures[location_id] = 0
        return
    _consecutive_failures[location_id] = _consecutive_failures.get(location_id, 0) + 1
    count = _consecutive_failures[location_id]
    if count >= FAILURE_ALERT_THRESHOLD:
        locs = get_locations()
        loc_name = next((l["name"] for l in locs if l["id"] == location_id), f"ID:{location_id}")
        alert_text = (
            f"⚠️ 静默失败告警\n"
            f"地点: {loc_name}\n"
            f"连续失败: {count} 次\n"
            f"最近错误: {error_msg}\n"
            f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"请检查天气API Token和网络连接"
        )
        log.warning(f"静默失败告警: {loc_name} 连续失败 {count} 次")
        try:
            push_message(alert_text)
            audit("silent_failure_alert", loc_name, f"连续失败{count}次: {error_msg}", "alert")
        except Exception as e:
            log.error(f"告警推送失败: {e}")
        _consecutive_failures[location_id] = 0


# ==================== 免费 API 自动刷新 ===================
_free_api_cache = {}  # {location_key: {"data": weather, "time": datetime}}

def refresh_free_api_sources():
    """定时刷新免费 API 数据源（Open-Meteo/GFS/ECMWF），独立于彩云天气"""
    global _free_api_cache
    locations = get_locations(enabled_only=True)
    if not locations:
        return
    
    free_providers = [WEATHER_PROVIDERS[n] for n in FREE_PROVIDER_NAMES if n in WEATHER_PROVIDERS]
    
    for loc in locations:
        loc_key = f"{loc['lng']},{loc['lat']}"
        results = []
        for provider in free_providers:
            try:
                weather, err = provider.fetch(loc["lng"], loc["lat"], None, timeout=API_TIMEOUT)
                if weather:
                    weather["_provider"] = provider.name
                    weather["_source_label"] = provider.display_name
                    results.append(weather)
                    log.debug(f"[免费API刷新] {provider.display_name} 成功")
            except Exception as e:
                log.warning(f"[免费API刷新] {provider.display_name} 失败: {e}")
        
        if results:
            # 加权融合免费源数据（ECMWF 精度最高，权重最大）
            # 权重分配：ECMWF=0.5, ICON=0.4, GFS/JMA/GEM=0.3, Open-Meteo=0.2
            provider_weights = {"ecmwf": 0.5, "icon": 0.4, "gfs": 0.3,
                                "jma": 0.3, "gem": 0.3, "openmeteo": 0.2}
            merged = dict(results[0])
            merge_fields = ["cloudrate", "cloud_high", "cloud_mid", "cloud_low",
                           "humidity", "wind", "temperature", "pressure",
                           "pm2_5", "pm10", "dust", "ozone", "no2", "us_aqi", "visibility"]
            
            for field in merge_fields:
                weighted_sum = 0.0
                total_weight = 0.0
                for r in results:
                    val = r.get(field)
                    if val is not None:
                        provider_name = r.get("_provider", "openmeteo")
                        weight = provider_weights.get(provider_name, 0.2)
                        weighted_sum += val * weight
                        total_weight += weight
                if total_weight > 0:
                    merged[field] = round(weighted_sum / total_weight, 1)
            
            merged["_free_sources"] = [r.get("_source_label") for r in results]
            merged["_free_count"] = len(results)
            _free_api_cache[loc_key] = {"data": merged, "time": datetime.now()}
            log.info(f"[免费API刷新] {loc['name']}: 加权合并 {len(results)} 源 (权重: ECMWF=0.5 ICON=0.4 GFS/JMA/GEM=0.3 Open-Meteo=0.2) 高云={merged.get('cloud_high')}% 低云={merged.get('cloud_low')}%")


def get_cached_free_data(lng, lat, max_age_minutes=30):
    """获取缓存的免费 API 数据，如果超过 max_age_minutes 则返回 None"""
    loc_key = f"{lng},{lat}"
    cached = _free_api_cache.get(loc_key)
    if not cached:
        return None
    age = (datetime.now() - cached["time"]).total_seconds() / 60
    if age > max_age_minutes:
        return None
    return cached["data"]


# ==================== 多源数据交叉验证 ===================
def _fetch_single_provider(provider, lng, lat, timeout):
    """单个 provider 的并行拉取封装"""
    token = _get_provider_token(provider)
    if not token and provider.name not in FREE_PROVIDER_NAMES:
        return None
    try:
        weather, err = provider.fetch(lng, lat, token, timeout=timeout)
        if weather:
            weather["_provider"] = provider.name
            weather["_source_label"] = provider.display_name
            return ("ok", weather)
        return ("err", f"{provider.display_name}: {err}")
    except Exception as e:
        return ("err", f"{provider.display_name}: {e}")


def fetch_weather_multi(lng, lat, timeout=API_TIMEOUT):
    """多源天气数据交叉验证：并行从所有可用API获取数据，取可用源的加权均值"""
    primary = get_weather_provider()
    results = []
    errors = []

    # 收集所有可用 provider（主源 + 所有有 token 的备选 + 免费源）
    try_order = [primary.name]
    for name in PROVIDER_FALLBACK_ORDER:
        if name != primary.name:
            p = WEATHER_PROVIDERS.get(name)
            if p:
                token = _get_provider_token(p)
                if token or p.name in FREE_PROVIDER_NAMES:
                    try_order.append(name)

    log.info(f"多源验证: 并行尝试数据源 {try_order}")

    # 检查是否有新鲜的免费 API 缓存数据
    free_api_max_age = int(get_config("free_api_refresh_minutes", "30"))
    cached_free = get_cached_free_data(lng, lat, max_age_minutes=free_api_max_age)

    # 分离：免费源（可用缓存）vs 需要并行调 API 的源
    free_providers = [n for n in try_order if n in FREE_PROVIDER_NAMES]
    paid_providers = [n for n in try_order if n not in FREE_PROVIDER_NAMES]

    # 免费源有缓存时直接使用，不重复调用
    if cached_free and free_providers:
        cached_free["_provider"] = "free_merged"
        cached_free["_source_label"] = "+".join(cached_free.get("_free_sources", ["免费源"]))
        results.append(cached_free)
        log.info(f"多源验证: 免费源使用缓存数据 (age < {free_api_max_age}分钟)")

    # 需要调 API 的 provider（付费源 + 无缓存的免费源）
    api_providers = []
    for name in paid_providers:
        p = WEATHER_PROVIDERS.get(name)
        if p:
            api_providers.append(p)
    for name in free_providers:
        if not cached_free:
            p = WEATHER_PROVIDERS.get(name)
            if p:
                api_providers.append(p)

    # 并行调用所有 API
    if api_providers:
        with ThreadPoolExecutor(max_workers=min(len(api_providers), 8)) as executor:
            futures = {executor.submit(_fetch_single_provider, p, lng, lat, timeout): p for p in api_providers}
            for future in as_completed(futures, timeout=timeout + 5):
                try:
                    result = future.result()
                    if result is None:
                        continue
                    status, data = result
                    if status == "ok":
                        results.append(data)
                        log.info(f"多源验证: {data['_source_label']} 成功 (云量={data.get('cloudrate')}%)")
                    else:
                        errors.append(data)
                        log.warning(f"多源验证: {data}")
                except Exception as e:
                    p = futures[future]
                    errors.append(f"{p.display_name}: {e}")
                    log.warning(f"多源验证: {p.display_name} 异常: {e}")

    if not results:
        return None, f"所有源均失败: {'; '.join(errors)}"

    if len(results) == 1:
        results[0]["_fetched_at"] = time.time()
        return results[0], None

    # 多源数据：加权均值（与 refresh_free_api_sources 统一策略）
    provider_weights = {"caiyun": 0.4, "qweather": 0.4, "visualcrossing": 0.3, "windy": 0.3,
                        "tomorrow": 0.3, "weatherapi": 0.3, "owm": 0.3,
                        "ecmwf": 0.5, "icon": 0.4, "gfs": 0.3, "jma": 0.3, "gem": 0.3,
                        "openmeteo": 0.2, "free_merged": 0.3}
    merged = dict(results[0])
    merge_fields = ["cloudrate", "cloud_high", "cloud_mid", "cloud_low",
                    "humidity", "wind", "visibility", "precipitation", "temperature", "pressure",
                    "pm2_5", "pm10", "dust", "ozone", "no2", "us_aqi",
                    "precip_probability"]
    for field in merge_fields:
        weighted_sum = 0.0
        total_weight = 0.0
        for r in results:
            val = r.get(field)
            if val is not None:
                pname = r.get("_provider", "openmeteo")
                weight = provider_weights.get(pname, 0.2)
                weighted_sum += val * weight
                total_weight += weight
        if total_weight > 0:
            merged[field] = round(weighted_sum / total_weight, 1)

    # 基于融合数据修正 skycon（主源天气描述可能与多源融合结果矛盾）
    merged_cloud = merged.get("cloudrate", 50)
    merged_precip = merged.get("precipitation", 0) or 0
    merged_cloud_high = merged.get("cloud_high", 50)
    merged_cloud_low = merged.get("cloud_low", 50)
    cur_skycon = merged.get("skycon", "")
    rain_keywords = ["雨", "drizzle", "rain", "shower", "毛毛雨"]
    is_rainy = any(kw in cur_skycon for kw in rain_keywords)
    if is_rainy and merged_cloud < 40 and merged_precip < 0.1:
        if merged_cloud_high < 20 and merged_cloud_low < 20:
            merged["skycon"] = "晴"
        elif merged_cloud < 20:
            merged["skycon"] = "晴"
        else:
            merged["skycon"] = "多云"
        log.info(f"多源修正: skycon '{cur_skycon}' → '{merged['skycon']}' (融合云量={merged_cloud}% 降水={merged_precip}mm/h)")
    elif is_rainy and merged_cloud < 60 and merged_precip < 0.2:
        merged["skycon"] = "多云"
        log.info(f"多源修正: skycon '{cur_skycon}' → '多云' (融合云量={merged_cloud}% 降水={merged_precip}mm/h)")

    sources = [r.get("_source_label", r.get("_provider", "?")) for r in results]
    merged["_provider"] = "+".join([r.get("_provider", "?") for r in results])
    merged["_source_label"] = "+".join(sources)
    merged["_fetched_at"] = time.time()
    log.info(f"多源验证: 加权合并 {len(results)} 个源 {sources}")
    return merged, None


# （富文本推送已移至 pusher.py）


# ==================== 配置导入/导出 ===================
def export_all_config():
    """导出全部配置为 JSON（凭证+地点+config+推送模板）"""
    data = {
        "version": 1,
        "exported_at": int(time.time()),
        "credentials": {},
        "locations": [],
        "config": {},
        "push_template": "",
        "morning_template": "",
    }
    # 凭证（脱敏：token 只导出标记，不导出值）
    creds = get_all_credentials()
    for k, v in creds.items():
        # webhook URL 脱敏：只保留域名部分
        if "webhook" in k or "sendkey" in k or "token" in k:
            data["credentials"][k] = {"configured": True, "preview": v[:8] + "..." if len(v) > 8 else "***"}
        else:
            data["credentials"][k] = {"configured": True, "value": v}
    # 地点
    data["locations"] = get_locations()
    # config
    data["config"] = get_all_config()
    # 推送模板
    data["push_template"] = get_config("push_template", DEFAULT_PUSH_TEMPLATE)
    data["morning_template"] = get_config("morning_template", DEFAULT_MORNING_TEMPLATE)
    return data


def import_config(data, import_credentials=False):
    """从 JSON 导入配置（默认不导入凭证）"""
    results = {"credentials": 0, "locations": 0, "config": 0, "templates": 0}
    # 导入凭证（仅当明确请求且数据中包含完整值）
    if import_credentials and "credentials" in data:
        for k, v in data["credentials"].items():
            if isinstance(v, dict) and v.get("value"):
                set_credential(k, v["value"])
                results["credentials"] += 1
    # 导入地点
    if "locations" in data:
        for loc in data["locations"]:
            if loc.get("name") and loc.get("lng") and loc.get("lat"):
                add_location(loc["name"], loc["lng"], loc["lat"])
                results["locations"] += 1
    # 导入 config
    if "config" in data:
        for k, v in data["config"].items():
            set_config(k, v)
            results["config"] += 1
    # 导入模板
    if data.get("push_template"):
        set_config("push_template", data["push_template"])
        results["templates"] += 1
    if data.get("morning_template"):
        set_config("morning_template", data["morning_template"])
        results["templates"] += 1
    return results


# （Webhook 连通性预检已移至 pusher.py）


# ==================== 升级/回滚 ===================

# 备份时排除的目录（运行时数据和依赖）
BACKUP_EXCLUDE_DIRS = {"data", "logs", "_versions", "backups", "__pycache__", ".git", "venv", "node_modules", ".deps"}
# 上传允许的文件（项目核心文件）
UPGRADE_ALLOWED_FILES = {
    "bot.py", "providers.py", "scoring.py", "pusher.py",
    "web.py", "templates.html",
    "requirements.txt", "install.sh", "start.sh", "stop.sh", "run_tests.sh",
    "migrate.py", "README.md",
    "manifest.json", "sw.js",
    "tests/test_scoring.py", "tests/test_pusher.py", "tests/test_web_api.py",
    "tests/test_fetcher.py", "tests/test_e2e.py", "tests/test_backup.py",
    "tests/test_migrate.py",
}


def _get_project_files():
    """获取项目中所有需要备份的文件（相对路径列表）"""
    files = []
    for p in BASE_DIR.rglob("*"):
        if p.is_file():
            rel = p.relative_to(BASE_DIR)
            # 排除运行时目录
            parts = rel.parts
            if parts and parts[0] in BACKUP_EXCLUDE_DIRS:
                continue
            # 排除 PID 文件、secret key 等
            if p.name in ("bot.pid", ".secret_key", "*.pyc"):
                continue
            files.append(str(rel).replace("\\", "/"))
    return files


def backup_version(cleanup=True):
    """
    备份整个项目代码（排除 data/logs/_versions/backups 等运行时目录）。
    每次强制使用时间戳命名，绝不互相覆盖。
    备份完成后自动清理，只保留最近 3 个版本（cleanup=False 时跳过清理）。
    """
    label = f"backup_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}"
    target = VERSIONS_DIR / label
    target.mkdir(parents=True, exist_ok=True)
    
    count = 0
    for rel_path in _get_project_files():
        src = BASE_DIR / rel_path
        dst = target / rel_path
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)
        count += 1
    
    log.info(f"备份完成: {label} ({count} 个文件)")
    
    # 自动清理：只保留最近 3 个备份（回滚前备份时不清理，避免删掉要回滚的版本）
    if cleanup:
        _cleanup_old_versions(keep=3)
    
    return label


def _cleanup_old_versions(keep=3):
    """清理旧版本，只保留最近 keep 个"""
    try:
        all_versions = sorted(
            [p for p in VERSIONS_DIR.iterdir() if p.is_dir()],
            key=lambda p: p.stat().st_mtime,
            reverse=True
        )
        to_delete = all_versions[keep:]
        for v in to_delete:
            shutil.rmtree(v)
            log.info(f"自动清理旧版本: {v.name}")
    except Exception as e:
        log.warning(f"清理旧版本失败: {e}")


def list_versions():
    """列出所有历史版本（按时间倒序）"""
    versions = []
    for p in sorted(VERSIONS_DIR.iterdir(), reverse=True):
        if p.is_dir():
            stat = p.stat()
            # 递归统计所有文件
            all_files = list(p.rglob("*"))
            files_only = [f for f in all_files if f.is_file()]
            file_names = [str(f.relative_to(p)).replace("\\", "/") for f in files_only]
            size_kb = round(sum(f.stat().st_size for f in files_only) / 1024, 1)
            versions.append({
                "name": p.name,
                "created_at": datetime.fromtimestamp(stat.st_mtime).isoformat(),
                "size_kb": size_kb,
                "files": file_names[:10],  # 最多显示 10 个
                "file_count": len(files_only),
            })
    return versions


def restore_version(version_name):
    """回滚到指定版本（恢复所有文件）"""
    target = VERSIONS_DIR / version_name
    if not target.exists() or not target.is_dir():
        return False, f"版本 {version_name} 不存在"
    # 回滚前先备份当前（不清理旧版本，避免删掉要回滚的目标版本）
    backup_version(cleanup=False)
    count = 0
    skipped = 0
    for src in target.rglob("*"):
        if src.is_file():
            rel = src.relative_to(target)
            # 跳过运行时目录和依赖目录（防止覆盖正在使用的文件）
            parts = rel.parts
            if parts and parts[0] in BACKUP_EXCLUDE_DIRS:
                skipped += 1
                continue
            dst = BASE_DIR / str(rel)
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, dst)
            count += 1
    log.info(f"回滚到 {version_name} ({count} 个文件, 跳过 {skipped} 个)")
    return True, f"已回滚到 {version_name}（{count} 个文件）"


def delete_version(version_name):
    """删除指定历史版本"""
    target = VERSIONS_DIR / version_name
    if not target.exists() or not target.is_dir():
        return False, f"版本 {version_name} 不存在"
    shutil.rmtree(target)
    log.info(f"删除版本: {version_name}")
    return True, f"已删除 {version_name}"


def apply_uploaded_files(uploaded):
    """
    应用上传的文件。
    uploaded: dict[filename_or_relpath, bytes]

    流程：
    1. 校验文件名（必须在白名单内）
    2. 备份整个项目
    3. 写入新文件
    4. 返回备份名
    """
    bad = set(uploaded.keys()) - UPGRADE_ALLOWED_FILES
    if bad:
        return False, f"不允许的文件: {bad}\n允许: {', '.join(sorted(UPGRADE_ALLOWED_FILES))}"

    backup_name = backup_version()
    for fname, content in uploaded.items():
        target = BASE_DIR / fname
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)
    log.info(f"应用 {len(uploaded)} 个文件，备份={backup_name}")
    return True, backup_name


# ==================== 进程管理 ===================
def is_running():
    """检查 bot 进程是否在运行"""
    if not PID_FILE.exists():
        return False
    try:
        pid = int(PID_FILE.read_text().strip())
        # 如果 PID 文件里是自己，不算"已有进程在运行"（避免 nohup 竞态）
        if pid == os.getpid():
            return False
        # Linux: kill -0 检测进程存在
        os.kill(pid, 0)
        return True
    except (OSError, ValueError):
        return False


def get_pid():
    if PID_FILE.exists():
        try:
            return int(PID_FILE.read_text().strip())
        except ValueError:
            return None
    return None


def write_pid():
    PID_FILE.write_text(str(os.getpid()))


def cleanup_pid():
    try:
        PID_FILE.unlink()
    except FileNotFoundError:
        pass


# ==================== 调度器 ===================
# 全局变量：存储今天的日落推送定时器
_sunset_timer = None
_sunset_info = {}  # 存储日落信息供 Web 查询


def get_sunset_info():
    """获取当前日落调度信息（供 Web 接口查询）"""
    return _sunset_info.copy()


def _has_any_weather_token():
    """检查是否配置了任何天气 API token"""
    for name in PROVIDER_FALLBACK_ORDER:
        provider = WEATHER_PROVIDERS.get(name)
        if not provider:
            continue
        if provider.name == "openmeteo":
            return True  # Open-Meteo 不需要 token
        token = _get_provider_token(provider)
        if token:
            return True
    return False


_morning_task_running = False

def morning_task():
    """早上任务：拉取天气预览 + 计算日落时间 + 调度傍晚推送"""
    global _sunset_timer, _sunset_info, _morning_task_running
    if _morning_task_running:
        log.warning("早上任务已在运行中，跳过本次执行")
        return
    _morning_task_running = True
    try:
        _morning_task_inner()
    finally:
        _morning_task_running = False

def _morning_task_inner():
    """早上任务的实际执行逻辑"""
    global _sunset_timer, _sunset_info
    log.info("=== 早上任务开始 ===")
    
    locations = get_locations(enabled_only=True)
    if not locations:
        log.warning("没有启用的地点")
        return
    if not _has_any_weather_token():
        log.warning("未配置任何天气 API token，请至少配置一个")
        return
    
    # 读取日落前多少分钟推送（可配置，默认50分钟）
    sunset_offset = int(get_config("sunset_offset_minutes", "50"))
    
    _sunset_info = {"morning_time": datetime.now().strftime("%H:%M"), "locations": []}
    
    for loc in locations:
        # 在每个地点开始处理前记录当前时间（避免 API 调用延迟影响 Timer 精度）
        now = datetime.now()
        
        # 早上任务：强制刷新 API（每天第1次调用）
        # 先清除旧缓存，确保新数据写入时旧数据已删除
        clear_weather_cache(loc["id"])
        if get_config("cross_validation", False):
            weather, err = fetch_weather_multi(loc["lng"], loc["lat"])
            source = weather.get("_source_label", "multi") if weather else "multi(error)"
            if weather:
                set_weather_cache(loc["id"], weather, source=source)
                log.info(f"[{loc['name']}] 多源交叉验证模式 数据源: {source}")
        else:
            weather, source, err = fetch_weather_cached(loc["id"], loc["lng"], loc["lat"], force_refresh=True)
        if err and not weather:
            log.warning(f"[{loc['name']}] 天气获取失败: {err}")
            continue
        if weather:
            log.info(f"[{loc['name']}] 天气数据来源: {source}")

        # 获取日落时间（优先用 API 时区，fallback 经度推算）
        sunset_dt, sunset_source = get_sunset_time_for_location(loc["lng"], loc["lat"], weather=weather)
        push_time = sunset_dt - timedelta(minutes=sunset_offset)

        # 补充日出时间（优先用 API 时区）
        tz_off = weather.get("timezone_offset") if weather else None
        sunrise_dt = _calc_sunrise_time(loc["lat"], loc["lng"], timezone_offset=tz_off)
        if weather and not weather.get("sunrise"):
            weather["sunrise"] = sunrise_dt.strftime("%H:%M")
        if weather and not weather.get("sunset"):
            weather["sunset"] = sunset_dt.strftime("%H:%M")

        log.info(f"[{loc['name']}] 日出: {sunrise_dt.strftime('%H:%M')} 日落: {sunset_dt.strftime('%H:%M')} (来源: {sunset_source})")
        log.info(f"[{loc['name']}] 计划推送时间: {push_time.strftime('%H:%M')} (日落前{sunset_offset}分钟)")

        _sunset_info["locations"].append({
            "name": loc["name"],
            "id": loc["id"],
            "sunset_time": sunset_dt.strftime("%H:%M"),
            "sunrise_time": sunrise_dt.strftime("%H:%M"),
            "push_time": push_time.strftime("%H:%M"),
            "sunset_offset": sunset_offset,
            "source": sunset_source,
        })

        # 早间推送：使用早间模板发送全天天气播报
        if weather:
            result = score_sunset(weather)
            morning_tpl = get_config("morning_push_template", DEFAULT_MORNING_TEMPLATE)
            morning_text = format_push_message(
                weather, result, loc["name"], sunset_dt.strftime("%H:%M"),
                source, template=morning_tpl
            )
            push_results = push_message(morning_text)
            for ch, r in push_results.items():
                record_push(loc["id"], result["score"], ch, r["success"], r["message"])
            log.info(f"[{loc['name']}] 早间播报已推送 (评分: {result['score']})")

        # 检查推送时间是否在未来（使用循环开始时记录的 now，避免 API 延迟）
        push_time_str = push_time.strftime("%H:%M")
        if push_time <= now:
            log.warning(f"[{loc['name']}] 推送时间 {push_time_str} 已过，跳过傍晚推送")
            continue
        
        # 使用 schedule 注册傍晚推送（替代 threading.Timer，重启后不会丢失）
        # 先取消已有的傍晚推送任务，避免重复注册
        for job in list(schedule.jobs):
            if getattr(job, '_evening_push_tag', False):
                schedule.cancel_job(job)
        
        def _scheduled_evening_push(loc=loc):
            _evening_push_for_location(loc)
        
        job = schedule.every().day.at(push_time_str).do(_scheduled_evening_push)
        job._evening_push_tag = True  # 标记用于后续取消
        log.info(f"[{loc['name']}] 已用 schedule 注册傍晚推送，计划 {push_time_str} 执行")
    
    log.info("=== 早上任务完成 ===")


def _evening_push_for_location(loc):
    """傍晚推送任务（独立函数，可被 schedule 或 morning_task 调用）"""
    log.info(f"=== 傍晚推送任务 [{loc['name']}] ===")
    # 傍晚任务：清除旧缓存后强制刷新 API
    clear_weather_cache(loc["id"])
    if get_config("cross_validation", False):
        # 推送前先强制刷新免费 API 数据，确保最新
        log.info(f"[{loc['name']}] 傍晚推送前强制刷新免费API...")
        refresh_free_api_sources()
        weather, err = fetch_weather_multi(loc["lng"], loc["lat"])
        src = weather.get("_source_label", "multi") if weather else "multi(error)"
        log.info(f"[{loc['name']}] 多源交叉验证模式 数据源: {src}")
        # 将多源融合结果写入缓存，供 Web 界面展示
        if weather:
            set_weather_cache(loc["id"], weather, source=src)
    else:
        weather, src, err = fetch_weather_cached(loc["id"], loc["lng"], loc["lat"], force_refresh=True)
    if err and not weather:
        log.warning(f"[{loc['name']}] {err}")
        check_silent_failure(loc["id"], False, err or "天气数据获取失败")
        return
    check_silent_failure(loc["id"], True)
    result = score_sunset(weather)
    score = result["score"]
    sunset_t = _sunset_info.get("locations", [{}])[0].get("sunset_time", "未知")
    log.info(f"[{loc['name']}] 评分: {score} 推荐: {result['recommend']} blocked: {result.get('blocked', False)} 日落: {sunset_t} 数据源: {src}")
    
    if result.get("blocked"):
        cloud = classify_cloud_type(weather)
        skycon = weather.get("skycon", "未知")
        blocked_text = (
            f"🌫️ 晚霞取消 - {loc['name']}\n"
            f"今日天气: {skycon}，{cloud['desc']}\n"
            f"🌇 日落时间: {sunset_t}（但不可见）\n"
            f"💡 {cloud['tip']}"
        )
        text = blocked_text
    else:
        text = format_push_message(weather, result, loc["name"], sunset_t, src)
    results = push_message(text)
    for ch, r in results.items():
        record_push(loc["id"], score, ch, r["success"], r["message"])


_scheduled_task_running = False

def run_scheduled_task():
    """执行一次完整的晚霞检查+推送流程（使用缓存数据，始终推送结果）"""
    global _scheduled_task_running
    if _scheduled_task_running:
        log.warning("调度任务已在运行中，跳过本次执行")
        return
    _scheduled_task_running = True
    try:
        _run_scheduled_task_inner()
    finally:
        _scheduled_task_running = False

def _run_scheduled_task_inner():
    """调度任务的实际执行逻辑"""
    log.info("=== 调度任务开始 ===")
    locations = get_locations(enabled_only=True)
    if not locations:
        log.warning("没有启用的地点")
        return
    if not _has_any_weather_token():
        log.warning("未配置任何天气 API token，请至少配置一个")
        return

    for loc in locations:
        # 傍晚推送：获取天气数据用于评分（支持交叉验证）
        if get_config("cross_validation", False):
            weather, err = fetch_weather_multi(loc["lng"], loc["lat"])
            source = weather.get("_source_label", "multi") if weather else "multi(error)"
            if weather:
                set_weather_cache(loc["id"], weather, source=source)
        else:
            # 使用缓存数据，不额外消耗 API 配额
            weather, source, err = fetch_weather_cached(loc["id"], loc["lng"], loc["lat"], force_refresh=False)
        if err and not weather:
            log.warning(f"[{loc['name']}] {err}")
            continue
        result = score_sunset(weather)
        score = result["score"]
        log.info(f"[{loc['name']}] 评分: {score} 推荐: {result['recommend']}")

        sunset_dt, _ = get_sunset_time_for_location(loc["lng"], loc["lat"], weather=weather)
        # 使用模板生成推送消息
        text = format_push_message(weather, result, loc["name"], sunset_dt.strftime("%H:%M"), source)
        results = push_message(text)
        for ch, r in results.items():
            record_push(loc["id"], score, ch, r["success"], r["message"])


# ==================== 调度时间配置 ===================
def get_schedule_times():
    """获取调度时间列表（去重）"""
    times = get_config("schedule_times", None)
    if times is None:
        # 默认时间
        return ["08:00", "17:00"]
    # 去重并保持顺序
    seen = set()
    deduped = []
    for t in times:
        if t not in seen:
            seen.add(t)
            deduped.append(t)
    return deduped


def set_schedule_times(times):
    """设置调度时间列表"""
    # 验证格式
    for t in times:
        if not isinstance(t, str) or len(t) != 5 or t[2] != ':':
            return False, f"无效时间格式: {t}，应为 HH:MM"
        try:
            h, m = int(t[:2]), int(t[3:])
            if h < 0 or h > 23 or m < 0 or m > 59:
                return False, f"无效时间: {t}"
        except ValueError:
            return False, f"无效时间: {t}"
    set_config("schedule_times", times)
    return True, "已保存"


# ==================== 启动初始化 ===================
def _startup_init():
    """启动时初始化：从缓存读取数据并注册傍晚推送，缓存为空时主动拉取"""
    global _sunset_info
    locations = get_locations(enabled_only=True)
    if not locations:
        return
    
    now = datetime.now()
    morning_time = get_config("morning_task_time", "08:00")
    morning_hour, morning_min = map(int, morning_time.split(":"))
    morning_dt = now.replace(hour=morning_hour, minute=morning_min, second=0, microsecond=0)
    
    cache = get_all_weather_cache()
    if not cache:
        # 缓存为空时：如果已过早上任务时间，立即执行早上任务
        if now >= morning_dt:
            log.info("[启动初始化] 缓存为空且已过早上任务时间，立即执行早上任务")
            morning_task()
            return
        else:
            log.info(f"[启动初始化] 缓存为空，等待 {morning_time} 早上任务拉取数据")
            return
    
    sunset_offset = int(get_config("sunset_offset_minutes", "50"))
    _sunset_info = {"morning_time": now.strftime("%H:%M"), "locations": []}
    
    for loc in locations:
        # 只读缓存，不拉取 API
        weather, source, err = fetch_weather_cached(loc["id"], loc["lng"], loc["lat"], force_refresh=False)
        if not weather:
            continue
        
        # 计算日落时间和傍晚推送时间
        try:
            sunset_dt, sunset_source = get_sunset_time_for_location(loc["lng"], loc["lat"], weather=weather)
            push_time = sunset_dt - timedelta(minutes=sunset_offset)
            push_time_str = push_time.strftime("%H:%M")
            
            # 补充日出时间
            tz_off = weather.get("timezone_offset") if weather else None
            sunrise_dt = _calc_sunrise_time(loc["lat"], loc["lng"], timezone_offset=tz_off)
            if weather and not weather.get("sunrise"):
                weather["sunrise"] = sunrise_dt.strftime("%H:%M")
            if weather and not weather.get("sunset"):
                weather["sunset"] = sunset_dt.strftime("%H:%M")
            
            log.info(f"[启动初始化] {loc['name']} 日落: {sunset_dt.strftime('%H:%M')} 计划推送: {push_time_str}")
            
            _sunset_info["locations"].append({
                "name": loc["name"],
                "id": loc["id"],
                "sunset_time": sunset_dt.strftime("%H:%M"),
                "sunrise_time": sunrise_dt.strftime("%H:%M"),
                "push_time": push_time_str,
                "sunset_offset": sunset_offset,
                "source": sunset_source,
            })
            
            # 如果推送时间还在未来，用 schedule 注册
            if push_time > now:
                # 先取消已有的傍晚推送任务，避免重复注册
                for job in list(schedule.jobs):
                    if getattr(job, '_evening_push_tag', False):
                        schedule.cancel_job(job)
                
                def _scheduled_evening_push(loc=loc):
                    _evening_push_for_location(loc)
                job = schedule.every().day.at(push_time_str).do(_scheduled_evening_push)
                job._evening_push_tag = True  # 标记用于后续取消
                log.info(f"[启动初始化] 已注册傍晚推送 {push_time_str} ({loc['name']})")
            else:
                log.info(f"[启动初始化] 推送时间 {push_time_str} 已过，跳过 ({loc['name']})")
        except Exception as e:
            log.error(f"[启动初始化] {loc['name']} 计算日落时间失败: {e}")


# ==================== 启动入口 ===================
def main():
    """主入口"""
    init_db()

    # 检查是否已有 bot 进程在运行，如果有则主动杀掉旧进程
    if is_running():
        old_pid = get_pid()
        # 防止杀自己（nohup 竞态：shell 先写了 PID 文件）
        if old_pid and old_pid != os.getpid():
            log.warning(f"检测到 bot 进程已在运行 (PID {old_pid})，正在终止旧进程...")
            try:
                os.kill(old_pid, signal.SIGTERM)
                # 等待旧进程退出（最多 3 秒）
                for _ in range(6):
                    time.sleep(0.5)
                    try:
                        os.kill(old_pid, 0)
                    except OSError:
                        break
                else:
                    # 还没退出，强制杀
                    os.kill(old_pid, signal.SIGKILL)
                    log.warning(f"旧进程 (PID {old_pid}) 强制终止")
                time.sleep(0.5)
            except OSError:
                pass  # 进程已不存在
            # 清理残留 PID 文件
            cleanup_pid()

    write_pid()

    # 注册信号
    signal.signal(signal.SIGTERM, lambda *_: cleanup_and_exit())
    signal.signal(signal.SIGINT, lambda *_: cleanup_and_exit())

    log.info("bot 启动（智能日落模式）")

    # 启动时执行一次维护清理
    run_maintenance()

    # 调度任务：
    # - 早上 08:00 运行 morning_task（计算日落 + 调度傍晚推送）
    # - 用户自定义的固定时间点仍保留 run_scheduled_task 作为补充
    morning_time = get_config("morning_task_time", "08:00")
    schedule.every().day.at(morning_time).do(morning_task)
    log.info(f"早上任务调度: {morning_time}")

    # 用户自定义的额外固定时间点（去重，且排除早上任务时间）
    schedule_times = get_schedule_times()
    registered_times = set()
    for t in schedule_times:
        if t != morning_time and t not in registered_times:
            registered_times.add(t)
            schedule.every().day.at(t).do(run_scheduled_task)
            log.info(f"额外调度时间: {t}")

    # 免费 API 定时刷新（默认每 30 分钟，可配置 free_api_refresh_minutes）
    free_refresh_interval = int(get_config("free_api_refresh_minutes", "30"))
    schedule.every(free_refresh_interval).minutes.do(refresh_free_api_sources)
    log.info(f"免费API自动刷新: 每 {free_refresh_interval} 分钟 (Open-Meteo/GFS/ECMWF)")

    # 每日维护任务：凌晨 4:00 清理过期日志 + 数据库瘦身
    schedule.every().day.at("04:00").do(run_maintenance)
    log.info(f"每日维护: 04:00 (日志保留{LOG_RETENTION_DAYS}天, 推送历史保留{PUSH_HISTORY_MAX_COUNT}条)")

    # 启动时初始化：如果缓存为空，拉取天气数据并注册傍晚推送
    _startup_init()

    log.info("等待定时任务触发")

    while True:
        schedule.run_pending()
        # 智能睡眠：距离下一个任务越远，轮询间隔越大（最大 30s）
        next_run = schedule.idle_seconds()
        if next_run is None:
            time.sleep(30)  # 无任务，长睡眠
        elif next_run > 60:
            time.sleep(30)
        elif next_run > 10:
            time.sleep(5)
        else:
            time.sleep(1)


def cleanup_and_exit():
    log.info("bot 退出")
    cleanup_pid()
    sys.exit(0)


if __name__ == "__main__":
    main()
