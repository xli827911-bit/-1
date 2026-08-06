#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
sunset-bot v6 - providers.py
天气 API 多平台抽象层：基类 + 12 个 Provider + 注册表
从 bot.py 拆分，保持接口不变。
"""
import math
import logging
import requests
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# 显式导出：bot.py 通过 from providers import * 获取所有公开符号
__all__ = [
    "WeatherProvider", "API_TIMEOUT", "get_http_session",
    "CaiyunProvider", "OpenWeatherMapProvider", "QWeatherProvider",
    "WeatherAPIProvider", "TomorrowIOProvider", "OpenMeteoProvider",
    "OpenMeteoModelProvider", "GFSProvider", "ECMWFProvider", "ICONProvider",
    "JMAProvider", "GEMProvider", "VisualCrossingProvider", "WindyProvider",
    "WEATHER_PROVIDERS", "FREE_PROVIDER_NAMES", "PROVIDER_FALLBACK_ORDER",
]

# 共享 logger（与 bot.py 同一名称，共享同一 logger 实例）
log = logging.getLogger("sunset-bot")

# 超时设置
API_TIMEOUT = 10

# 共享 HTTP Session（连接池复用 + 自动重试）
_http_session = requests.Session()
_retry_strategy = Retry(
    total=3,
    backoff_factor=1,
    status_forcelist=[500, 502, 503, 504],
    allowed_methods=["GET", "POST"],
)
_http_session.mount("https://", HTTPAdapter(max_retries=_retry_strategy))
_http_session.mount("http://", HTTPAdapter(max_retries=_retry_strategy))


def get_http_session():
    """获取共享 HTTP Session（带自动重试）"""
    return _http_session


# ==================== 天气 API 多平台抽象层 ===================
class WeatherProvider:
    """天气 API 提供者基类（抽象接口）"""
    name = "base"
    display_name = "Base Provider"
    
    def fetch(self, lng, lat, token, timeout=API_TIMEOUT):
        """
        拉取实时天气数据
        返回: (weather_dict, error_string)
        weather_dict 标准字段:
            cloudrate: 云量 (0-100%)
            cloud_low: 低云量 (0-100%)  — 晚霞预测核心
            cloud_mid: 中云量 (0-100%)  — 晚霞预测核心
            cloud_high: 高云量 (0-100%) — 晚霞预测核心
            humidity: 湿度 (0-100%)
            wind: 风速 (m/s)
            visibility: 能见度 (km)
            precipitation: 降水强度 (mm/h)
            temperature: 温度 (°C)
            datetime: 数据时间
        扩展字段（部分 API 支持）:
            uv_index: 紫外线指数
            pressure: 气压 (hPa)
            dew_point: 露点温度 (°C)
            sunrise: 日出时间 "HH:MM"
            sunset: 日落时间 "HH:MM"
            temp_high: 全天最高温 (°C)
            temp_low: 全天最低温 (°C)
            precip_probability: 降水概率 (0-100%)
            skycon: 天气现象描述
            aqi: 空气质量指数
            wind_direction: 风向 (度)
        """
        raise NotImplementedError

    def fetch_daily(self, lng, lat, token, timeout=API_TIMEOUT):
        """获取每日汇总数据（日出日落/最高最低温/降水概率/AQI 等），返回 (daily_dict, error)"""
        return None, "该 API 不支持每日汇总"


class CaiyunProvider(WeatherProvider):
    """彩云天气 API"""
    name = "caiyun"
    display_name = "彩云天气"
    API_URL = "https://api.caiyunapp.com/v2.5/{token}/{lng},{lat}/weather.json?dailysteps=1"

    # 彩云天气现象 → 中文描述
    SKYCON_MAP = {
        "CLEAR_DAY": "晴", "CLEAR_NIGHT": "晴", "PARTLY_CLOUDY_DAY": "多云",
        "PARTLY_CLOUDY_NIGHT": "多云", "CLOUDY": "阴", "LIGHT_HAZE": "轻度雾霾",
        "MODERATE_HAZE": "中度雾霾", "HEAVY_HAZE": "重度雾霾", "LIGHT_RAIN": "小雨",
        "MODERATE_RAIN": "中雨", "HEAVY_RAIN": "大雨", "STORM_RAIN": "暴雨",
        "FOG": "雾", "LIGHT_SNOW": "小雪", "MODERATE_SNOW": "中雪",
        "HEAVY_SNOW": "大雪", "STORM_SNOW": "暴雪", "DUST": "浮尘",
        "SAND": "沙尘", "WIND": "大风",
    }

    def fetch(self, lng, lat, token, timeout=API_TIMEOUT):
        url = self.API_URL.format(token=token, lng=lng, lat=lat)
        try:
            resp = get_http_session().get(url, timeout=timeout)
            if resp.status_code != 200:
                return None, f"彩云 API 返回 {resp.status_code}"
            data = resp.json()
            if data.get("status") != "ok":
                return None, f"彩云 API 错误: {data.get('status')}"
            result = data.get("result", {})
            hourly = result.get("hourly", {})
            daily = result.get("daily", {})
            if not hourly:
                return None, "彩云 API 无 hourly 数据"

            first_idx = 0
            raw_humidity = hourly["humidity"][first_idx]["value"]
            humidity = raw_humidity * 100 if raw_humidity <= 1.0 else raw_humidity

            raw_visibility = hourly["visibility"][first_idx]["value"]
            visibility = raw_visibility / 1000 if raw_visibility > 100 else raw_visibility

            wind_data = hourly["wind"][first_idx]
            wind_speed = wind_data.get("speed", 0) if isinstance(wind_data, dict) else wind_data

            raw_cloudrate = hourly["cloudrate"][first_idx]["value"]
            cloudrate = raw_cloudrate * 100 if raw_cloudrate <= 1.0 else raw_cloudrate

            weather = {
                "cloudrate": round(cloudrate, 1),
                "humidity": round(humidity, 1),
                "wind": round(wind_speed, 1),
                "visibility": round(visibility, 1),
                "precipitation": hourly["precipitation"][first_idx]["value"],
                "temperature": round(hourly["temperature"][first_idx]["value"], 1),
                "datetime": hourly["temperature"][first_idx]["datetime"],
                "lat": lat,  # 供季节修正系数使用
            }

            # 提取多层云量数据（如果 API 提供）
            for cloud_key in ["cloud_high", "cloud_mid", "cloud_low"]:
                cloud_list = hourly.get(cloud_key, [])
                if cloud_list and first_idx < len(cloud_list):
                    raw_val = cloud_list[first_idx].get("value")
                    if raw_val is not None:
                        # 如果是 0-1 范围，转换为百分比
                        weather[cloud_key] = round(raw_val * 100 if raw_val <= 1.0 else raw_val, 1)

            # 提取露点温度（如果 API 提供）
            dew_list = hourly.get("dew_point", [])
            if dew_list and first_idx < len(dew_list):
                weather["dew_point"] = round(dew_list[first_idx].get("value", 0), 1)

            # 体感温度
            apparent_list = hourly.get("apparent_temperature", [])
            if apparent_list:
                weather["apparent_temperature"] = round(apparent_list[first_idx].get("value", 0), 1)

            # 天气预报摘要
            keypoint = result.get("forecast_keypoint", "")
            if keypoint:
                weather["forecast_keypoint"] = keypoint

            # 从 realtime 部分提取更丰富的实时数据
            realtime = result.get("realtime", {})
            if realtime:
                # 实时体感温度（优先用 realtime，比 hourly 更精准）
                if "apparent_temperature" in realtime:
                    weather["apparent_temperature"] = round(realtime["apparent_temperature"], 1)
                # 实时气压
                if "pressure" in realtime:
                    weather["pressure"] = round(realtime["pressure"] / 100, 1)  # Pa→hPa
                # 实时风向
                rt_wind = realtime.get("wind", {})
                if isinstance(rt_wind, dict) and "direction" in rt_wind:
                    weather["wind_direction"] = round(rt_wind["direction"], 1)
                # 实时空气质量（完整指标）
                aq = realtime.get("air_quality", {})
                if aq:
                    if "pm25" in aq:
                        weather["pm25"] = aq["pm25"]
                    if "pm10" in aq:
                        weather["pm10"] = aq["pm10"]
                    if "o3" in aq:
                        weather["o3"] = aq["o3"]
                    if "so2" in aq:
                        weather["so2"] = aq["so2"]
                    if "no2" in aq:
                        weather["no2"] = aq["no2"]
                    if "co" in aq:
                        weather["co"] = aq["co"]
                    aqi_obj = aq.get("aqi", {})
                    if isinstance(aqi_obj, dict):
                        if "chn" in aqi_obj:
                            weather["aqi"] = aqi_obj["chn"]
                        if "usa" in aqi_obj:
                            weather["aqi_usa"] = aqi_obj["usa"]
                    desc = aq.get("description", {})
                    if isinstance(desc, dict):
                        if "chn" in desc:
                            weather["aqi_desc"] = desc["chn"]
                # 实时天气现象（realtime 的 skycon 比 daily 的更精准）
                if "skycon" in realtime:
                    weather["skycon"] = self.SKYCON_MAP.get(realtime["skycon"], realtime["skycon"])
                    weather["skycon_raw"] = realtime["skycon"]
                # 实时生活指数（优先用 realtime 的）
                rt_life = realtime.get("life_index", {})
                if rt_life:
                    uv = rt_life.get("ultraviolet", {})
                    if uv:
                        weather["uv_index"] = int(uv.get("index", 0))
                        weather["uv_desc"] = uv.get("desc", "")
                    comfort = rt_life.get("comfort", {})
                    if comfort:
                        weather["comfort_desc"] = comfort.get("desc", "")

            # 从 daily 数据中提取丰富信息并合并
            if daily:
                extra = self._extract_daily(daily)
                weather.update(extra)

                # === 构建日落时段的完整天气数据（用于评分） ===
                sunset_str = extra.get("sunset", "")
                if sunset_str:
                    try:
                        sunset_h, sunset_m = map(int, sunset_str.split(":"))
                        sunset_minutes = sunset_h * 60 + sunset_m
                        # 找到最接近日落的 hourly 索引
                        best_idx = 0
                        best_diff = 9999
                        for i, entry in enumerate(hourly.get("temperature", [])):
                            dt = entry.get("datetime", "")
                            if "T" in dt:
                                time_part = dt.split("T")[1][:5]
                                h, m = map(int, time_part.split(":"))
                                diff = abs(h * 60 + m - sunset_minutes)
                                if diff < best_diff:
                                    best_diff = diff
                                    best_idx = i

                        # 用日落时段的 hourly 数据构建 sunset_weather
                        sw = {}
                        # 云量
                        cr_list = hourly.get("cloudrate", [])
                        if best_idx < len(cr_list):
                            raw = cr_list[best_idx]["value"]
                            sw["cloudrate"] = round(raw * 100 if raw <= 1.0 else raw, 1)
                        # 多层云量
                        for ck in ["cloud_high", "cloud_mid", "cloud_low"]:
                            cl = hourly.get(ck, [])
                            if best_idx < len(cl):
                                raw = cl[best_idx].get("value")
                                if raw is not None:
                                    sw[ck] = round(raw * 100 if raw <= 1.0 else raw, 1)
                        # 天气现象
                        sky_list = hourly.get("skycon", [])
                        if best_idx < len(sky_list):
                            sv = sky_list[best_idx].get("value", "")
                            sw["skycon"] = self.SKYCON_MAP.get(sv, sv)
                            sw["skycon_raw"] = sv
                        # 湿度
                        hum_list = hourly.get("humidity", [])
                        if best_idx < len(hum_list):
                            raw = hum_list[best_idx]["value"]
                            sw["humidity"] = round(raw * 100 if raw <= 1.0 else raw, 1)
                        # 风速
                        wind_list = hourly.get("wind", [])
                        if best_idx < len(wind_list):
                            wd = wind_list[best_idx]
                            sw["wind"] = {"speed": round(wd.get("speed", 0) if isinstance(wd, dict) else wd, 1)}
                        # 能见度
                        vis_list = hourly.get("visibility", [])
                        if best_idx < len(vis_list):
                            raw = vis_list[best_idx]["value"]
                            sw["visibility"] = round(raw / 1000 if raw > 100 else raw, 1)
                        # 降水
                        precip_list = hourly.get("precipitation", [])
                        if best_idx < len(precip_list):
                            sw["precipitation"] = precip_list[best_idx]["value"]
                        # 温度 & 露点
                        temp_list = hourly.get("temperature", [])
                        if best_idx < len(temp_list):
                            sw["temperature"] = round(temp_list[best_idx]["value"], 1)
                        dew_list = hourly.get("dew_point", [])
                        if best_idx < len(dew_list):
                            sw["dew_point"] = round(dew_list[best_idx]["value"], 1)

                        if sw:
                            weather["sunset_weather"] = sw
                            weather["sunset_cloudrate"] = sw.get("cloudrate")
                            weather["sunset_visibility"] = sw.get("visibility")
                            log.info(f"[彩云] 日落时段({sunset_str})预报: 云量={sw.get('cloudrate')}% 天气={sw.get('skycon')} 高云={sw.get('cloud_high')} 低云={sw.get('cloud_low')}")
                    except Exception as e:
                        log.warning(f"[彩云] 提取日落时段数据失败: {e}")

            return weather, None
        except requests.Timeout:
            return None, "彩云 API 超时"
        except Exception as e:
            return None, f"彩云 API 异常: {e}"

    def _extract_daily(self, daily):
        """从彩云 daily 数据提取丰富信息"""
        extra = {}
        try:
            # 日出日落
            astro = daily.get("astro", [{}])
            if astro:
                sr = astro[0].get("sunrise", {})
                ss = astro[0].get("sunset", {})
                if sr.get("time"):
                    extra["sunrise"] = sr["time"]
                if ss.get("time"):
                    extra["sunset"] = ss["time"]
            # 最高最低温
            temp = daily.get("temperature", [{}])
            if temp:
                extra["temp_high"] = temp[0].get("max", 0)
                extra["temp_low"] = temp[0].get("min", 0)
            # 降水概率
            precip = daily.get("precipitation", [{}])
            if precip:
                extra["precip_probability"] = precip[0].get("probability", 0)
            # 白天风速风向
            wind_day = daily.get("wind_08h_20h", daily.get("wind", [{}]))
            if wind_day:
                avg_wind = wind_day[0].get("avg", {})
                if isinstance(avg_wind, dict):
                    extra["wind_direction"] = avg_wind.get("direction", 0)
            # 湿度
            hum = daily.get("humidity", [{}])
            if hum:
                extra["humidity_day"] = round(hum[0].get("avg", 0) * 100, 1) if hum[0].get("avg", 0) <= 1 else hum[0].get("avg", 0)
            # 云量日平均
            cr = daily.get("cloudrate", [{}])
            if cr:
                extra["cloudrate_day"] = round(cr[0].get("avg", 0), 2)
            # 气压
            pres = daily.get("pressure", [{}])
            if pres:
                avg_p = pres[0].get("avg", 0)
                extra["pressure"] = round(avg_p / 100, 1) if avg_p > 1000 else avg_p  # Pa→hPa
            # 能见度
            vis = daily.get("visibility", [{}])
            if vis:
                extra["visibility_day"] = vis[0].get("avg", 0)
            # 辐射
            dswrf = daily.get("dswrf", [{}])
            if dswrf:
                extra["dswrf"] = dswrf[0].get("avg", 0)
            # AQI
            aq = daily.get("air_quality", {})
            aqi_list = aq.get("aqi", [{}])
            if aqi_list:
                extra["aqi"] = aqi_list[0].get("avg", {}).get("chn", 0)
            pm25_list = aq.get("pm25", [{}])
            if pm25_list:
                extra["pm25"] = pm25_list[0].get("avg", 0)
            # 天气现象
            skycon = daily.get("skycon", daily.get("skycon_08h_20h", [{}]))
            if skycon:
                val = skycon[0].get("value", "")
                extra["skycon"] = self.SKYCON_MAP.get(val, val)
                extra["skycon_raw"] = val
            # 生活指数
            life = daily.get("life_index", {})
            uv = life.get("ultraviolet", [{}])
            if uv:
                extra["uv_index"] = int(uv[0].get("index", 0))
                extra["uv_desc"] = uv[0].get("desc", "")
            comfort = life.get("comfort", [{}])
            if comfort:
                extra["comfort_desc"] = comfort[0].get("desc", "")
            car_wash = life.get("carWashing", [{}])
            if car_wash:
                extra["car_washing"] = car_wash[0].get("desc", "")
            dressing = life.get("dressing", [{}])
            if dressing:
                extra["dressing"] = dressing[0].get("desc", "")
            cold_risk = life.get("coldRisk", [{}])
            if cold_risk:
                extra["cold_risk"] = cold_risk[0].get("desc", "")
        except Exception:
            pass
        return extra

    def fetch_daily(self, lng, lat, token, timeout=API_TIMEOUT):
        """获取彩云每日汇总数据"""
        url = self.API_URL.format(token=token, lng=lng, lat=lat)
        try:
            resp = get_http_session().get(url, timeout=timeout)
            if resp.status_code != 200:
                return None, f"彩云 API 返回 {resp.status_code}"
            data = resp.json()
            if data.get("status") != "ok":
                return None, f"彩云 API 错误"
            daily = data.get("result", {}).get("daily", {})
            if not daily:
                return None, "无 daily 数据"
            return self._extract_daily(daily), None
        except Exception as e:
            return None, f"彩云每日数据异常: {e}"


class OpenWeatherMapProvider(WeatherProvider):
    """OpenWeatherMap API（示例扩展，后期可启用）"""
    name = "openweathermap"
    display_name = "OpenWeatherMap"
    API_URL = "https://api.openweathermap.org/data/3.0/onecall?lat={lat}&lon={lng}&appid={token}&units=metric&exclude=minutely,alerts"

    def fetch(self, lng, lat, token, timeout=API_TIMEOUT):
        url = self.API_URL.format(token=token, lng=lng, lat=lat)
        try:
            resp = get_http_session().get(url, timeout=timeout)
            if resp.status_code != 200:
                return None, f"OWM API 返回 {resp.status_code}"
            data = resp.json()
            hourly = data.get("hourly", [])
            if not hourly:
                return None, "OWM API 无 hourly 数据"
            h = hourly[0]
            # daily 数据（如果存在）
            daily_arr = data.get("daily", [])
            daily = daily_arr[0] if daily_arr else {}
            weather = {
                "cloudrate": h.get("clouds", 0),
                "humidity": h.get("humidity", 0),
                "wind": round(h.get("wind_speed", 0), 1),
                "visibility": round(h.get("visibility", 10000) / 1000, 1),  # m → km
                "precipitation": h.get("rain", {}).get("1h", 0) if isinstance(h.get("rain"), dict) else 0,
                "temperature": round(h.get("temp", 0), 1),
                "datetime": datetime.fromtimestamp(h.get("dt", 0)).isoformat(),
                "uv_index": h.get("uvi", 0),
                "pressure": h.get("pressure", 1013),
                "dew_point": round(h.get("dew_point", 0), 1),
                "wind_direction": h.get("wind_deg", 0),
            }
            # 从 daily 数据提取日出日落和温度范围
            if daily:
                weather["sunrise"] = datetime.fromtimestamp(daily.get("sunrise", 0)).strftime("%H:%M") if daily.get("sunrise") else ""
                weather["sunset"] = datetime.fromtimestamp(daily.get("sunset", 0)).strftime("%H:%M") if daily.get("sunset") else ""
                weather["temp_high"] = round(daily.get("temp", {}).get("max", 0), 1) if isinstance(daily.get("temp"), dict) else 0
                weather["temp_low"] = round(daily.get("temp", {}).get("min", 0), 1) if isinstance(daily.get("temp"), dict) else 0
                weather["precip_probability"] = int(daily.get("pop", 0) * 100)
                weather["skycon"] = daily.get("weather", [{}])[0].get("description", "") if daily.get("weather") else ""
            return weather, None
        except requests.Timeout:
            return None, "OWM API 超时"
        except Exception as e:
            return None, f"OWM API 异常: {e}"


class QWeatherProvider(WeatherProvider):
    """和风天气 API（国内推荐）"""
    name = "qweather"
    display_name = "和风天气"
    API_URL = "https://devapi.qweather.com/v7/weather/now?location={lng},{lat}&key={token}"

    def fetch(self, lng, lat, token, timeout=API_TIMEOUT):
        url = self.API_URL.format(token=token, lng=lng, lat=lat)
        try:
            resp = get_http_session().get(url, timeout=timeout)
            if resp.status_code != 200:
                return None, f"和风天气 API 返回 {resp.status_code}"
            data = resp.json()
            if data.get("code") != "200":
                return None, f"和风天气 API 错误: code={data.get('code')}"
            now = data.get("now", {})
            if not now:
                return None, "和风天气 API 无数据"

            # 和风天气能见度单位是 km
            visibility = float(now.get("vis", "10"))

            weather = {
                "cloudrate": float(now.get("cloud", "0")),
                "humidity": float(now.get("humidity", "50")),
                "wind": round(float(now.get("windSpeed", "0")), 1),
                "visibility": round(visibility, 1),
                "precipitation": float(now.get("precip", "0")),
                "temperature": float(now.get("temp", "0")),
                "datetime": now.get("obsTime", datetime.now().isoformat()),
                "pressure": float(now.get("pressure", "1013")),
                "dew_point": float(now.get("dew", "0")),
                "wind_direction": float(now.get("windDir", "0")) if now.get("windDir", "").isdigit() else 0,
                "skycon": now.get("text", ""),
                "feels_like": float(now.get("feelsLike", "0")),
            }
            return weather, None
        except requests.Timeout:
            return None, "和风天气 API 超时"
        except Exception as e:
            return None, f"和风天气 API 异常: {e}"


class WeatherAPIProvider(WeatherProvider):
    """WeatherAPI.com（国际通用，有免费额度）"""
    name = "weatherapi"
    display_name = "WeatherAPI.com"
    API_URL = "https://api.weatherapi.com/v1/current.json?key={token}&q={lat},{lng}&aqi=no"

    def fetch(self, lng, lat, token, timeout=API_TIMEOUT):
        url = self.API_URL.format(token=token, lng=lng, lat=lat)
        try:
            resp = get_http_session().get(url, timeout=timeout)
            if resp.status_code != 200:
                return None, f"WeatherAPI 返回 {resp.status_code}"
            data = resp.json()
            current = data.get("current", {})
            if not current:
                return None, "WeatherAPI 无数据"

            # 能见度单位是 km
            visibility = float(current.get("vis_km", "10"))

            weather = {
                "cloudrate": float(current.get("cloud", "0")),
                "humidity": float(current.get("humidity", "50")),
                "wind": round(float(current.get("wind_kph", "0")) / 3.6, 1),  # kph → m/s
                "visibility": round(visibility, 1),
                "precipitation": float(current.get("precip_mm", "0")),
                "temperature": float(current.get("temp_c", "0")),
                "uv_index": float(current.get("uv", "0")),
                "pressure": float(current.get("pressure_mb", "1013")),
                "dew_point": float(current.get("dewpoint_c", "0")),
                "wind_direction": current.get("wind_degree", 0),
                "feels_like": float(current.get("feelslike_c", "0")),
                "skycon": current.get("condition", {}).get("text", ""),
                "datetime": current.get("last_updated_epoch", "") and datetime.fromtimestamp(current["last_updated_epoch"]).isoformat() or datetime.now().isoformat(),
            }
            return weather, None
        except requests.Timeout:
            return None, "WeatherAPI 超时"
        except Exception as e:
            return None, f"WeatherAPI 异常: {e}"


class TomorrowIOProvider(WeatherProvider):
    """Tomorrow.io（高精度分钟级天气，晚霞预测优秀）"""
    name = "tomorrowio"
    display_name = "Tomorrow.io"
    API_URL = "https://api.tomorrow.io/v4/weather/realtime?location={lat},{lng}&apikey={token}&units=metric"

    def fetch(self, lng, lat, token, timeout=API_TIMEOUT):
        url = self.API_URL.format(token=token, lng=lng, lat=lat)
        try:
            resp = get_http_session().get(url, timeout=timeout)
            if resp.status_code != 200:
                return None, f"Tomorrow.io 返回 {resp.status_code}"
            data = resp.json()
            values = data.get("data", {}).get("values", {})
            if not values:
                return None, "Tomorrow.io 无数据"

            weather = {
                "cloudrate": float(values.get("cloudCover", "0")),
                "humidity": float(values.get("humidity", "50")),
                "wind": round(float(values.get("windSpeed", "0")), 1),
                "visibility": round(float(values.get("visibility", "10")), 1),  # km
                "precipitation": float(values.get("precipitationIntensity", "0")),
                "temperature": float(values.get("temperature", "0")),
                "uv_index": float(values.get("uvIndex", "0")),
                "pressure": float(values.get("pressureSurfaceLevel", "1013")),
                "dew_point": float(values.get("dewPoint", "0")),
                "wind_direction": float(values.get("windDirection", "0")),
                "datetime": data.get("data", {}).get("time", datetime.now().isoformat()),
            }
            return weather, None
        except requests.Timeout:
            return None, "Tomorrow.io 超时"
        except Exception as e:
            return None, f"Tomorrow.io 异常: {e}"


class OpenMeteoProvider(WeatherProvider):
    """Open-Meteo（免费开源，无需 API Key）"""
    name = "openmeteo"
    display_name = "Open-Meteo (免费)"
    API_URL = "https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lng}&current=temperature_2m,relative_humidity_2m,cloud_cover,cloud_cover_low,cloud_cover_mid,cloud_cover_high,wind_speed_10m,wind_direction_10m,visibility,precipitation,surface_pressure,dew_point_2m,uv_index&daily=sunrise,sunset,temperature_2m_max,temperature_2m_min,precipitation_probability_max,weathercode&timezone=auto&forecast_days=1"
    AQI_URL = "https://air-quality-api.open-meteo.com/v1/air-quality?latitude={lat}&longitude={lng}&hourly=pm2_5,pm10,dust,ozone,nitrogen_dioxide,us_aqi&timezone=auto&forecast_days=1"

    def _wmo_to_text(self, code):
        """WMO 天气代码转中文"""
        wmo = {
            0: "晴", 1: "大部晴", 2: "多云", 3: "阴", 45: "雾", 48: "雾凇",
            51: "小毛毛雨", 53: "中毛毛雨", 55: "大毛毛雨",
            61: "小雨", 63: "中雨", 65: "大雨",
            71: "小雪", 73: "中雪", 75: "大雪",
            80: "小阵雨", 81: "中阵雨", 82: "大阵雨",
            95: "雷暴", 96: "雷暴+小冰雹", 99: "雷暴+大冰雹",
        }
        return wmo.get(code, f"代码{code}")

    def fetch(self, lng, lat, token, timeout=API_TIMEOUT):
        # Open-Meteo 不需要 token，但保持接口一致
        url = self.API_URL.format(lng=lng, lat=lat)
        try:
            resp = get_http_session().get(url, timeout=timeout)
            if resp.status_code != 200:
                return None, f"Open-Meteo 返回 {resp.status_code}"
            data = resp.json()
            current = data.get("current", {})
            if not current:
                return None, "Open-Meteo 无数据"

            # 能见度单位是 m，需要转为 km
            vis_m = float(current.get("visibility", 10000))
            # 风速单位是 km/h，需要转为 m/s
            wind_kmh = float(current.get("wind_speed_10m", 0))

            weather = {
                "cloudrate": float(current.get("cloud_cover", "0")),
                "cloud_low": float(current.get("cloud_cover_low", "0")),
                "cloud_mid": float(current.get("cloud_cover_mid", "0")),
                "cloud_high": float(current.get("cloud_cover_high", "0")),
                "humidity": float(current.get("relative_humidity_2m", "50")),
                "wind": round(wind_kmh / 3.6, 1),
                "visibility": round(vis_m / 1000, 1),
                "precipitation": float(current.get("precipitation", "0")),
                "temperature": float(current.get("temperature_2m", "0")),
                "uv_index": float(current.get("uv_index", "0")),
                "pressure": float(current.get("surface_pressure", "1013")),
                "dew_point": float(current.get("dew_point_2m", "0")),
                "wind_direction": float(current.get("wind_direction_10m", "0")),
                "datetime": current.get("time", datetime.now().isoformat()),
            }

            # 从 API 提取时区偏移（Open-Meteo timezone=auto 返回 utc_offset_seconds）
            utc_offset_sec = data.get("utc_offset_seconds")
            if utc_offset_sec is not None:
                weather["timezone_offset"] = round(int(utc_offset_sec) / 3600)

            # 从 daily 数据提取日出日落和温度范围
            daily = data.get("daily", {})
            if daily:
                sunrises = daily.get("sunrise", [])
                sunsets = daily.get("sunset", [])
                if sunrises:
                    # "2022-05-26T05:51" → "05:51"
                    weather["sunrise"] = sunrises[0].split("T")[1] if "T" in sunrises[0] else sunrises[0]
                if sunsets:
                    weather["sunset"] = sunsets[0].split("T")[1] if "T" in sunsets[0] else sunsets[0]
                temp_max = daily.get("temperature_2m_max", [])
                temp_min = daily.get("temperature_2m_min", [])
                if temp_max:
                    weather["temp_high"] = float(temp_max[0])
                if temp_min:
                    weather["temp_low"] = float(temp_min[0])
                precip_prob = daily.get("precipitation_probability_max", [])
                if precip_prob:
                    weather["precip_probability"] = int(precip_prob[0] or 0)
                weathercode = daily.get("weathercode", [])
                if weathercode:
                    weather["skycon"] = self._wmo_to_text(int(weathercode[0])) if weathercode[0] is not None else ""

            # 拉取空气质量数据（Air Quality API，免费无需 token）
            try:
                aqi_url = self.AQI_URL.format(lng=lng, lat=lat)
                aqi_resp = get_http_session().get(aqi_url, timeout=timeout)
                if aqi_resp.status_code == 200:
                    aqi_data = aqi_resp.json()
                    hourly = aqi_data.get("hourly", {})
                    times = hourly.get("time", [])
                    # 找到当前时间最接近的小时
                    now_str = datetime.now().strftime("%Y-%m-%dT%H:00")
                    best_idx = 0
                    for i, t in enumerate(times):
                        if t <= now_str:
                            best_idx = i
                    pm25_list = hourly.get("pm2_5", [])
                    pm10_list = hourly.get("pm10", [])
                    dust_list = hourly.get("dust", [])
                    o3_list = hourly.get("ozone", [])
                    no2_list = hourly.get("nitrogen_dioxide", [])
                    aqi_list = hourly.get("us_aqi", [])
                    if pm25_list and best_idx < len(pm25_list):
                        weather["pm2_5"] = round(pm25_list[best_idx] or 0, 1)
                    if pm10_list and best_idx < len(pm10_list):
                        weather["pm10"] = round(pm10_list[best_idx] or 0, 1)
                    if dust_list and best_idx < len(dust_list):
                        weather["dust"] = round(dust_list[best_idx] or 0, 1)
                    if o3_list and best_idx < len(o3_list):
                        weather["ozone"] = round(o3_list[best_idx] or 0, 1)
                    if no2_list and best_idx < len(no2_list):
                        weather["no2"] = round(no2_list[best_idx] or 0, 1)
                    if aqi_list and best_idx < len(aqi_list):
                        weather["us_aqi"] = round(aqi_list[best_idx] or 0)
                    log.info(f"Open-Meteo 空气质量: PM2.5={weather.get('pm2_5')} PM10={weather.get('pm10')} Dust={weather.get('dust')} AQI={weather.get('us_aqi')}")
                else:
                    log.warning(f"Open-Meteo Air Quality API 返回 {aqi_resp.status_code}")
            except Exception as aqi_err:
                log.warning(f"Open-Meteo 空气质量获取失败: {aqi_err}")

            return weather, None
        except requests.Timeout:
            return None, "Open-Meteo 超时"
        except Exception as e:
            return None, f"Open-Meteo 异常: {e}"


class OpenMeteoModelProvider(WeatherProvider):
    """Open-Meteo 模型预报公共基类（GFS/ECMWF 等共用）"""
    API_MODEL = ""  # 子类覆盖："gfs" 或 "ecmwf"

    def fetch(self, lng, lat, token, timeout=API_TIMEOUT):
        url = f"https://api.open-meteo.com/v1/{self.API_MODEL}?latitude={lat}&longitude={lng}&hourly=cloud_cover,cloud_cover_low,cloud_cover_mid,cloud_cover_high,temperature_2m,relative_humidity_2m,wind_speed_10m,surface_pressure&timezone=auto&forecast_days=1"
        try:
            resp = get_http_session().get(url, timeout=timeout)
            if resp.status_code != 200:
                return None, f"{self.display_name} 返回 {resp.status_code}"
            data = resp.json()
            hourly = data.get("hourly", {})
            times = hourly.get("time", [])
            if not times:
                return None, f"{self.display_name} 无数据"

            # 找到当前时间最接近的小时
            now_str = datetime.now().strftime("%Y-%m-%dT%H:00")
            best_idx = 0
            for i, t in enumerate(times):
                if t <= now_str:
                    best_idx = i

            wind_kmh = float(hourly.get("wind_speed_10m", [0])[best_idx] or 0)
            weather = {
                "cloudrate": float(hourly.get("cloud_cover", [0])[best_idx] or 0),
                "cloud_high": float(hourly.get("cloud_cover_high", [0])[best_idx] or 0),
                "cloud_mid": float(hourly.get("cloud_cover_mid", [0])[best_idx] or 0),
                "cloud_low": float(hourly.get("cloud_cover_low", [0])[best_idx] or 0),
                "humidity": float(hourly.get("relative_humidity_2m", [50])[best_idx] or 50),
                "wind": round(wind_kmh / 3.6, 1),
                "temperature": float(hourly.get("temperature_2m", [0])[best_idx] or 0),
                "pressure": float(hourly.get("surface_pressure", [1013])[best_idx] or 1013),
                "datetime": times[best_idx],
            }
            log.info(f"{self.display_name}: 云量={weather['cloudrate']}% 高云={weather['cloud_high']}% 低云={weather['cloud_low']}% 气压={weather['pressure']}hPa")
            return weather, None
        except requests.Timeout:
            return None, f"{self.display_name} 超时"
        except Exception as e:
            return None, f"{self.display_name} 异常: {e}"


class GFSProvider(OpenMeteoModelProvider):
    """NOAA GFS 全球预报模式（通过 Open-Meteo 免费接口）"""
    name = "gfs"
    display_name = "NOAA GFS"
    API_MODEL = "gfs"


class ECMWFProvider(OpenMeteoModelProvider):
    """ECMWF 欧洲中期天气预报中心（通过 Open-Meteo 免费接口，精度最高）"""
    name = "ecmwf"
    display_name = "ECMWF (高精度)"
    API_MODEL = "ecmwf"


class ICONProvider(OpenMeteoModelProvider):
    """德国气象局 DWD ICON 模式（通过 Open-Meteo 免费接口）"""
    name = "icon"
    display_name = "DWD ICON (德国)"
    API_MODEL = "dwd-icon"


class JMAProvider(OpenMeteoModelProvider):
    """日本气象厅 JMA 模式（通过 Open-Meteo 免费接口，东亚区域精度高）"""
    name = "jma"
    display_name = "JMA (日本)"
    API_MODEL = "jma"


class GEMProvider(OpenMeteoModelProvider):
    """加拿大气象局 GEM 模式（通过 Open-Meteo 免费接口）"""
    name = "gem"
    display_name = "GEM (加拿大)"
    API_MODEL = "gem"


class VisualCrossingProvider(WeatherProvider):
    """Visual Crossing Weather API（多层云量，晚霞预测最佳）"""
    name = "visualcrossing"
    display_name = "Visual Crossing"
    API_URL = "https://weather.visualcrossing.com/VisualCrossingWebAPI/rest/services/timeline/{lat},{lng}/{date}?key={token}&unitGroup=metric&include=current,fcsthourly12&elements=cloudcover,cloudcover_low,cloudcover_mid,cloudcover_high,humidity,visibility,precip,precipprob,temp,dew,sunset,sunrise,uvindex,windspeed,winddir,pressure&lang=zh"

    def fetch(self, lng, lat, token, timeout=API_TIMEOUT):
        url = self.API_URL.format(lng=lng, lat=lat, date=datetime.now().strftime("%Y-%m-%d"), token=token)
        try:
            resp = get_http_session().get(url, timeout=timeout)
            if resp.status_code != 200:
                return None, f"VisualCrossing 返回 {resp.status_code}"
            data = resp.json()
            current = data.get("currentConditions")
            if not current:
                return None, "VisualCrossing 无 currentConditions"

            # 风速单位是 km/h → m/s
            wind_raw = float(current.get("windspeed", "0"))
            weather = {
                "cloudrate": float(current.get("cloudcover", "0")),
                "cloud_low": float(current.get("cloudcover_low", "0") or "0"),
                "cloud_mid": float(current.get("cloudcover_mid", "0") or "0"),
                "cloud_high": float(current.get("cloudcover_high", "0") or "0"),
                "humidity": float(current.get("humidity", "50")),
                "wind": round(wind_raw / 3.6, 1),
                "visibility": float(current.get("visibility", "10")),
                "precipitation": float(current.get("precip", "0")),
                "temperature": float(current.get("temp", "0")),
                "uv_index": float(current.get("uvindex", "0")),
                "pressure": float(current.get("pressure", "1013")),
                "dew_point": float(current.get("dew", "0")),
                "wind_direction": float(current.get("winddir", "0")),
                "datetime": current.get("datetime", datetime.now().isoformat()),
            }

            # 从 hourly forecast 获取日出日落
            hours = data.get("days", [{}])[0].get("hours", []) if data.get("days") else []
            for h in hours:
                sr = h.get("sunrise")
                ss = h.get("sunset")
                if sr:
                    weather["sunrise"] = sr
                if ss:
                    weather["sunset"] = ss

            # 从 days 获取温度范围和降水概率
            days = data.get("days", [])
            if days:
                d = days[0]
                weather["temp_high"] = float(d.get("tempmax", "0") or "0")
                weather["temp_low"] = float(d.get("tempmin", "0") or "0")
                weather["precip_probability"] = int(float(d.get("precipprob", "0") or "0"))
                desc = d.get("description", "")
                if desc:
                    weather["skycon"] = desc

            return weather, None
        except requests.Timeout:
            return None, "VisualCrossing 超时"
        except Exception as e:
            return None, f"VisualCrossing 异常: {e}"


class WindyProvider(WeatherProvider):
    """Windy Point Forecast API（多层云量，晚霞预测优秀）"""
    name = "windy"
    display_name = "Windy"
    API_URL = "https://api.windy.com/api/point-forecast/v2"

    def fetch(self, lng, lat, token, timeout=API_TIMEOUT):
        # Windy 使用 POST 请求
        payload = {
            "lat": round(lat, 2),
            "lon": round(lng, 2),
            "model": "gfs",
            "parameters": ["temp", "rh", "wind", "lclouds", "mclouds", "hclouds",
                           "pressure", "precip", "dewpoint"],
            "levels": ["surface"],
            "key": token,
        }
        try:
            resp = get_http_session().post(self.API_URL, json=payload, timeout=timeout)
            if resp.status_code != 200:
                return None, f"Windy 返回 {resp.status_code}"
            data = resp.json()
            ts_list = data.get("ts", [])
            if not ts_list:
                return None, "Windy 无数据"

            # 找到当前时间对应的索引（Windy 返回毫秒时间戳）
            now_ms = int(datetime.now().timestamp() * 1000)
            idx = 0
            for i, t in enumerate(ts_list):
                if t <= now_ms:
                    idx = i
                else:
                    break

            def val(key, default=0):
                arr = data.get(key, [])
                if idx < len(arr) and arr[idx] is not None:
                    return float(arr[idx])
                return default

            # Windy 的风速单位是 m/s，风向由 u/v 向量计算
            wind_u = val("wind_u-surface")
            wind_v = val("wind_v-surface")
            import math
            wind_speed = math.sqrt(wind_u**2 + wind_v**2)
            wind_dir = (math.degrees(math.atan2(-wind_u, -wind_v)) + 360) % 360

            weather = {
                "cloudrate": val("lclouds-surface") + val("mclouds-surface") + val("hclouds-surface"),
                "cloud_low": val("lclouds-surface"),
                "cloud_mid": val("mclouds-surface"),
                "cloud_high": val("hclouds-surface"),
                "humidity": val("rh-surface", 50),
                "wind": round(wind_speed, 1),
                "wind_direction": round(wind_dir, 1),
                "visibility": 10.0,  # Windy GFS 模型不直接提供能见度
                "precipitation": val("past3precip-surface"),
                "temperature": val("temp-surface"),
                "pressure": val("pressure-surface", 1013),
                "dew_point": val("dewpoint-surface"),
                "datetime": datetime.fromtimestamp(ts_list[idx] / 1000).isoformat(),
                "_provider": self.name,
            }
            return weather, None
        except requests.Timeout:
            return None, "Windy 超时"
        except Exception as e:
            return None, f"Windy 异常: {e}"


# 天气 API 提供者注册表
WEATHER_PROVIDERS = {
    "caiyun": CaiyunProvider(),
    "qweather": QWeatherProvider(),
    "visualcrossing": VisualCrossingProvider(),
    "openweathermap": OpenWeatherMapProvider(),
    "weatherapi": WeatherAPIProvider(),
    "tomorrowio": TomorrowIOProvider(),
    "openmeteo": OpenMeteoProvider(),
    "windy": WindyProvider(),
    "gfs": GFSProvider(),
    "ecmwf": ECMWFProvider(),
    "icon": ICONProvider(),
    "jma": JMAProvider(),
    "gem": GEMProvider(),
}

# 免费数据源（无需 Token，走 Open-Meteo 免费接口）
FREE_PROVIDER_NAMES = ("openmeteo", "gfs", "ecmwf", "icon", "jma", "gem")

# 降级顺序：主 API 失败时依次尝试
PROVIDER_FALLBACK_ORDER = ["visualcrossing", "caiyun", "qweather", "tomorrowio", "openweathermap", "weatherapi", "openmeteo", "gfs", "ecmwf", "icon", "jma", "gem", "windy"]
