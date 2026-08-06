#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
sunset-bot v6 - scoring.py
晚霞评分算法 v3：多层云量 + 空气质量 + AI 校准
从 bot.py 拆分，保持接口不变。

依赖 bot.py 中的：
  - DEFAULT_THRESHOLDS（评分阈值常量）
  - _is_skycon_blocked（天气遮挡判断）
  - get_calibration_offset（AI 校准偏移量）
  - log（全局 logger）
"""
import time
import logging
from datetime import datetime

log = logging.getLogger("sunset-bot")

__all__ = [
    "score_sunset",
]


# ---------- 评分子函数（按维度拆分） ----------

def _score_high_cloud(cloud_high, high_coeff=1.0):
    """高云量评分（0-15分）：高云(卷云)是晚霞主角，越多越好"""
    ch = cloud_high or 0
    if ch <= 10:
        s = ch / 10 * 5
    elif ch <= 80:
        s = 5 + (ch - 10) / 70 * 10
    else:
        s = 15 - (ch - 80) / 20 * 3
    return round(s * high_coeff, 1)


def _score_mid_cloud(cloud_mid):
    """中云量评分（0-10分）：中云(高积云)创造鱼鳞/碎云纹理"""
    cm = cloud_mid or 0
    if cm <= 5:
        s = cm / 5 * 2
    elif cm <= 60:
        s = 2 + (cm - 5) / 55 * 8
    else:
        s = max(0, 10 - (cm - 60) / 40 * 8)
    return round(s, 1)


def _score_low_cloud(cloud_low, low_coeff=1.0):
    """低云惩罚（0 到 -15分）：低云过厚直接遮挡日落"""
    cl = cloud_low or 0
    if cl <= 40:
        s = 0
    elif cl <= 70:
        s = -(cl - 40) / 30 * 5
    elif cl <= 90:
        s = -5 - (cl - 70) / 20 * 5
    else:
        s = -10 - (cl - 90) / 10 * 5
    return round(s * low_coeff, 1)


def _score_cloudrate_fallback(cloudrate, thresholds):
    """向后兼容：无多层云量时使用总云量评分（0-40分）"""
    cr = cloudrate or 0
    if cr < thresholds["cloudrate_min"]:
        s = max(0, cr / thresholds["cloudrate_min"] * 40)
    elif cr > thresholds["cloudrate_max"]:
        s = max(0, (100 - cr) / (100 - thresholds["cloudrate_max"]) * 40)
    else:
        s = 40
    return round(s, 1)


def _score_humidity(humidity, thresholds):
    """湿度评分（0-15分）：30-70% 最佳"""
    hm = humidity if humidity is not None else 50
    if hm < thresholds["humidity_min"]:
        s = max(0, hm / thresholds["humidity_min"] * 15)
    elif hm > thresholds["humidity_max"]:
        s = max(0, (100 - hm) / (100 - thresholds["humidity_max"]) * 15)
    else:
        s = 15
    return round(s, 1)


def _score_wind(wind, thresholds):
    """风速评分（0-15分）：0-3 m/s 最佳"""
    wd = wind.get("speed", 0) if isinstance(wind, dict) else (wind or 0)
    if wd <= thresholds["wind_max"]:
        s = 15 * (1 - wd / thresholds["wind_max"] / 2)
    else:
        s = max(0, 15 - (wd - thresholds["wind_max"]) * 3)
    return round(s, 1)


def _score_visibility(visibility, thresholds):
    """能见度评分（0-15分）：>10 km 最佳"""
    vis = visibility if visibility is not None else 10
    if vis >= thresholds["visibility_min"]:
        s = 15
    else:
        s = 15 * (vis / thresholds["visibility_min"])
    return round(s, 1)


def _score_precipitation(precipitation):
    """无降水评分（0或5分）：降水>0则0分"""
    return 5 if (precipitation or 0) == 0 else 0


def _score_dew_gap(temp, dew_point):
    """露点差评分（0-10分）：temp-dew 在 5-15°C 最佳"""
    if dew_point is None or not temp:
        return 0, 0
    dew_gap = abs(temp - dew_point)
    if 5 <= dew_gap <= 15:
        s = 10
    elif dew_gap < 5:
        s = dew_gap / 5 * 10
    else:
        s = max(0, 10 - (dew_gap - 15) / 15 * 8)
    return round(s, 1), dew_gap


def _score_inversion_bonus(dew_gap, wind_speed, humidity):
    """逆温层加分（0或7分）：粉紫暮霭黄金条件"""
    if 8 <= dew_gap <= 15 and wind_speed < 3 and 60 <= humidity <= 85:
        return 7
    return 0


def _score_air_quality(pm25):
    """空气质量评分（0-10分）：PM2.5 10-50 μg/m³ 最佳"""
    if pm25 is None:
        return 0
    if pm25 < 5:
        s = pm25 / 5 * 3
    elif pm25 <= 15:
        s = 3 + (pm25 - 5) / 10 * 4
    elif pm25 <= 50:
        s = 7 + (pm25 - 15) / 35 * 3
    elif pm25 <= 100:
        s = max(0, 10 - (pm25 - 50) / 50 * 6)
    else:
        s = max(0, 4 - (pm25 - 100) / 100 * 4)
    return round(s, 1)


def _get_season_coefficients(month, lat):
    """根据月份和纬度返回季节修正系数 (high_coeff, low_coeff, season_label)"""
    if month in (11, 12, 1, 2):
        high_coeff = 1.2 if lat > 30 else 1.1
        low_coeff = 0.7
        season_label = "冬季"
    elif month in (6, 7, 8):
        high_coeff = 1.0
        low_coeff = 1.3
        season_label = "夏季"
    else:
        high_coeff = 1.0
        low_coeff = 1.0
        season_label = "春秋"
    return high_coeff, low_coeff, season_label


# ---------- 主评分函数 ----------

def score_sunset(weather, thresholds=None):
    """
    根据天气数据计算晚霞评分 0~100（v3 多层云量+空气质量算法）

    评分维度（加权）：
        高云量      15 分   高云(卷云)是晚霞主角，越多越好
        中云量      10 分   中云(高积云)创造鱼鳞/碎云纹理
        低云惩罚    -15分   低云过厚直接遮挡日落
        湿度        15 分   30-70% 最佳（露点差 5-15°C）
        风速        15 分   0-3 m/s 最佳
        能见度      15 分   >10 km 最佳
        无降水      5 分    必须为 0
        露点差      10 分   temp-dew 在 5-15°C 最佳（湿度适中）
        空气质量    10 分   PM2.5 10-50 μg/m³ 最佳（适度气溶胶增强色彩）
    总计：最高 110 分（截断为 100）

    向后兼容：如果没有多层云量数据，退回总云量评分
    """
    # 延迟导入避免循环依赖
    import bot
    if thresholds is None:
        thresholds = bot.DEFAULT_THRESHOLDS

    # === 优先使用日落时段的预报数据评分（而非当前时刻数据） ===
    sw = weather.get("sunset_weather")
    if sw:
        weather = {**weather, **sw}
        log.info(f"[评分] 使用日落时段预报数据: 云量={sw.get('cloudrate')}% 天气={sw.get('skycon')} 高云={sw.get('cloud_high')} 低云={sw.get('cloud_low')}")

    # === 季节修正系数（根据月份和纬度调整高云/低云权重） ===
    _month = datetime.now().month
    _lat = weather.get("lat", 30)
    _high_coeff, _low_coeff, _season_label = _get_season_coefficients(_month, _lat)

    score = 0
    breakdown = {}

    # 检查是否有多层云量数据
    has_multi_cloud = any(weather.get(k) is not None for k in ("cloud_low", "cloud_mid", "cloud_high"))

    if has_multi_cloud:
        # === 高云量 15 分（卷云/卷积云 → 火烧云、丝缕晚霞） ===
        s_high = _score_high_cloud(weather.get("cloud_high"), _high_coeff)
        score += s_high
        breakdown["cloud_high"] = s_high

        # === 中云量 10 分（高积云/层积云 → 鱼鳞晚霞） ===
        s_mid = _score_mid_cloud(weather.get("cloud_mid"))
        score += s_mid
        breakdown["cloud_mid"] = s_mid

        # === 低云惩罚 最多 -15 分（精细化 + 季节修正） ===
        s_low = _score_low_cloud(weather.get("cloud_low"), _low_coeff)
        score += s_low
        breakdown["cloud_low"] = s_low

        # 用总云量做兼容（向后兼容）
        cr = weather.get("cloudrate", 0)
    else:
        # === 向后兼容：无多层云量时使用总云量 40 分 ===
        s = _score_cloudrate_fallback(weather.get("cloudrate"), thresholds)
        score += s
        breakdown["cloudrate"] = s

    # === 湿度 15 分 ===
    s = _score_humidity(weather.get("humidity"), thresholds)
    score += s
    breakdown["humidity"] = s

    # === 风速 15 分 ===
    s = _score_wind(weather.get("wind"), thresholds)
    score += s
    breakdown["wind"] = s

    # === 能见度 15 分 ===
    s = _score_visibility(weather.get("visibility"), thresholds)
    score += s
    breakdown["visibility"] = s

    # === 无降水 5 分 ===
    s = _score_precipitation(weather.get("precipitation"))
    score += s
    breakdown["no_precip"] = s

    # === 露点差 10 分（温度-露点差值 5-15°C 最佳） ===
    temp = weather.get("temperature", 0)
    dew = weather.get("dew_point")
    s_dew, dew_gap = _score_dew_gap(temp, dew)
    if s_dew > 0 or dew_gap > 0:
        score += s_dew
        breakdown["dew_gap"] = s_dew

        # === 逆温层加分（粉紫暮霭黄金条件） ===
        _wind_speed = weather.get("wind", {"speed": 0}).get("speed", 0) if isinstance(weather.get("wind"), dict) else weather.get("wind", 0)
        _hum = weather.get("humidity", 50)
        s_inv = _score_inversion_bonus(dew_gap, _wind_speed, _hum)
        if s_inv > 0:
            score += s_inv
            breakdown["inversion_bonus"] = s_inv

    # === 空气质量 10 分（PM2.5 10-50 μg/m³ 最佳，适度气溶胶增强晚霞色彩） ===
    s_aqi = _score_air_quality(weather.get("pm2_5"))
    if s_aqi > 0:
        score += s_aqi
        breakdown["air_quality"] = s_aqi

    # === 数据时效衰减因子（缓存过久则扣分） ===
    fetched_at = weather.get("_fetched_at")
    if fetched_at:
        data_age_min = (time.time() - fetched_at) / 60
        if data_age_min > 60:
            decay = min(20, int((data_age_min - 60) / 30) * 5)
            score -= decay
            breakdown["data_decay"] = -decay
            log.info(f"[评分] 数据时效衰减: 缓存{data_age_min:.0f}分钟前, 扣{decay}分")
    data_stale_blocked = False
    if fetched_at and (time.time() - fetched_at) / 60 > 180:
        data_stale_blocked = True
        log.warning(f"[评分] 数据超过3小时，标记为blocked")

    # === AI 校准偏移（基于历史照片分析的中位数偏移） ===
    _loc_id = weather.get("location_id")
    _ai_offset = bot.get_calibration_offset(_loc_id)
    if _ai_offset != 0:
        score += _ai_offset
        breakdown["ai_calibration"] = _ai_offset

    # 限制分数范围
    score = max(0, min(100, score))

    # 硬过滤：降水 > 0 绝对不推荐
    precip = weather.get("precipitation", 0)
    score_ok = score >= thresholds["score_threshold"]
    no_precip = precip == 0
    is_recommend = score_ok and no_precip
    
    # 调试日志：显示推荐判断细节
    if not is_recommend:
        log.info(f"[评分调试] 推荐=False 原因: 分数={score} 阈值={thresholds['score_threshold']} 分数OK={score_ok} 降水={precip} 无降水OK={no_precip}")

    # 检查是否被雾/霾/雨雪等遮挡（blocked = 今天看不到日落）
    low_cloud_blocked = (weather.get("cloud_low") or 0) > 80
    # 有多层云量数据时，不用总云量>90%做硬拦截（彩云总云量偏保守，GFS/ECMWF分层数据更准确）
    if has_multi_cloud:
        cloudrate_blocked = False
    else:
        cloudrate_blocked = weather.get("cloudrate", 0) > 90
    is_blocked = bot._is_skycon_blocked(weather) or cloudrate_blocked or low_cloud_blocked or data_stale_blocked
    if is_blocked:
        is_recommend = False
        log.info(f"[评分调试] blocked=True 原因: skycon={bot._is_skycon_blocked(weather)} cloudrate_blocked={cloudrate_blocked} low_cloud_blocked={low_cloud_blocked} data_stale={data_stale_blocked}")

    # 记录季节信息到 breakdown
    breakdown["season"] = _season_label

    return {
        "score": round(score, 1),
        "breakdown": breakdown,
        "recommend": is_recommend,
        "blocked": is_blocked,
    }
