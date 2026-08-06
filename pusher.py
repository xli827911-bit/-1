#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
sunset-bot v6 - pusher.py
推送通道模块：5 通道推送 + 并行推送 + 富文本 + Webhook 预检
从 bot.py 拆分，保持接口不变。

依赖 bot.py 中的：
  - get_credential（凭证读取）
  - get_config（配置读取）
  - get_http_session（共享 HTTP Session）
  - PUSH_TIMEOUT（超时常量）
  - log（全局 logger）
"""
import logging
import requests

log = logging.getLogger("sunset-bot")

__all__ = [
    "push_wechat", "push_dingtalk", "push_feishu",
    "push_serverchan", "push_pushplus",
    "push_wechat_markdown", "push_feishu_rich",
    "push_message", "test_webhook",
]


# ==================== 基础推送通道 ===================

def push_wechat(text, webhook_url=None, timeout=None):
    """企业微信 webhook 推送（支持多个 webhook，每行一个）"""
    import bot
    if timeout is None:
        timeout = bot.PUSH_TIMEOUT
    if not webhook_url:
        webhook_url = bot.get_credential("wechat_webhook", "")
    if not webhook_url:
        return False, "未配置企业微信 webhook"
    
    # 支持多行 webhook URL（每行一个）
    urls = [u.strip() for u in webhook_url.split("\n") if u.strip()]
    if not urls:
        return False, "企业微信 webhook 为空"
    
    all_ok = True
    msgs = []
    for url in urls:
        try:
            resp = bot.get_http_session().post(
                url,
                json={"msgtype": "text", "text": {"content": text}},
                timeout=timeout,
            )
            if resp.status_code == 200 and resp.json().get("errcode") == 0:
                msgs.append(f"✓ {url[:40]}...")
            else:
                all_ok = False
                msgs.append(f"✗ {url[:40]}...: {resp.text[:100]}")
        except Exception as e:
            all_ok = False
            msgs.append(f"✗ {url[:40]}...: {e}")
    
    return all_ok, "; ".join(msgs)


def push_dingtalk(text, timeout=None):
    """钉钉 webhook 推送"""
    import bot
    if timeout is None:
        timeout = bot.PUSH_TIMEOUT
    webhook_url = bot.get_credential("dingtalk_webhook", "")
    if not webhook_url:
        return False, "未配置钉钉 webhook"
    try:
        resp = bot.get_http_session().post(
            webhook_url,
            json={"msgtype": "text", "text": {"content": text}},
            timeout=timeout,
        )
        if resp.status_code == 200:
            return True, "推送成功"
        return False, f"推送失败: {resp.text[:200]}"
    except Exception as e:
        return False, f"推送异常: {e}"


def push_feishu(text, timeout=None):
    """飞书 webhook 推送"""
    import bot
    if timeout is None:
        timeout = bot.PUSH_TIMEOUT
    webhook_url = bot.get_credential("feishu_webhook", "")
    if not webhook_url:
        return False, "未配置飞书 webhook"
    try:
        resp = bot.get_http_session().post(
            webhook_url,
            json={"msg_type": "text", "content": {"text": text}},
            timeout=timeout,
        )
        if resp.status_code == 200:
            return True, "推送成功"
        return False, f"推送失败: {resp.text[:200]}"
    except Exception as e:
        return False, f"推送异常: {e}"


def push_serverchan(text, timeout=None):
    """Server酱 推送到微信公众号"""
    import bot
    if timeout is None:
        timeout = bot.PUSH_TIMEOUT
    sendkey = bot.get_credential("serverchan_sendkey", "")
    if not sendkey:
        return False, "未配置 Server酱 SendKey"
    try:
        title = text.split("\n")[0][:100]  # 第一行作为标题
        resp = bot.get_http_session().post(
            f"https://sctapi.ftqq.com/{sendkey}.send",
            data={"title": title, "desp": text.replace("\n", "\n\n")},
            timeout=timeout,
        )
        if resp.status_code == 200:
            d = resp.json()
            if d.get("code") == 0:
                return True, "推送成功"
            return False, f"Server酱错误: {d.get('message', '未知')}"
        return False, f"Server酱返回 {resp.status_code}"
    except Exception as e:
        return False, f"Server酱异常: {e}"


def push_pushplus(text, timeout=None):
    """PushPlus 推送到微信公众号"""
    import bot
    if timeout is None:
        timeout = bot.PUSH_TIMEOUT
    token = bot.get_credential("pushplus_token", "")
    if not token:
        return False, "未配置 PushPlus Token"
    try:
        title = text.split("\n")[0][:100]
        resp = bot.get_http_session().post(
            "https://www.pushplus.plus/send",
            json={"token": token, "title": title, "content": text.replace("\n", "<br>"), "template": "html"},
            timeout=timeout,
        )
        if resp.status_code == 200:
            d = resp.json()
            if d.get("code") == 200:
                return True, "推送成功"
            return False, f"PushPlus错误: {d.get('msg', '未知')} (code={d.get('code')})"
        return False, f"PushPlus返回 {resp.status_code}: {resp.text[:100]}"
    except Exception as e:
        return False, f"PushPlus异常: {e}"


# ==================== 富文本推送 ===================

def push_wechat_markdown(text, webhook_url=None, timeout=None):
    """企业微信 Markdown 格式推送"""
    import bot
    if timeout is None:
        timeout = bot.PUSH_TIMEOUT
    if not webhook_url:
        webhook_url = bot.get_credential("wechat_webhook", "")
    if not webhook_url:
        return False, "未配置企业微信 webhook"

    urls = [u.strip() for u in webhook_url.split("\n") if u.strip()]
    all_ok = True
    msgs = []
    for url in urls:
        try:
            # 将纯文本转为简单 markdown
            md_text = text.replace("\n", "\n>")
            title = text.split("\n")[0][:50]
            resp = bot.get_http_session().post(
                url,
                json={"msgtype": "markdown", "markdown": {"content": md_text}},
                timeout=timeout,
            )
            if resp.status_code == 200 and resp.json().get("errcode") == 0:
                msgs.append(f"✓ {url[:40]}...")
            else:
                all_ok = False
                msgs.append(f"✗ {url[:40]}...: {resp.text[:100]}")
        except Exception as e:
            all_ok = False
            msgs.append(f"✗ {url[:40]}...: {e}")
    return all_ok, "; ".join(msgs)


def push_feishu_rich(text, timeout=None):
    """飞书富文本(interactive)卡片推送"""
    import bot
    if timeout is None:
        timeout = bot.PUSH_TIMEOUT
    webhook_url = bot.get_credential("feishu_webhook", "")
    if not webhook_url:
        return False, "未配置飞书 webhook"

    title = text.split("\n")[0][:50]
    # 构建飞书 post 富文本
    lines = text.split("\n")
    content_lines = []
    for line in lines:
        content_lines.append([{"tag": "text", "text": line}])

    payload = {
        "msg_type": "post",
        "content": {
            "post": {
                "zh_cn": {
                    "title": title,
                    "content": content_lines
                }
            }
        }
    }
    try:
        resp = bot.get_http_session().post(webhook_url, json=payload, timeout=timeout)
        if resp.status_code == 200:
            return True, "富文本推送成功"
        return False, f"推送失败: {resp.text[:200]}"
    except Exception as e:
        return False, f"推送异常: {e}"


# ==================== 并行多通道推送 ===================

def push_message(text, channels=None, rich_text=None):
    """
    推送消息到多个通道（并行执行）
    channels: ['wechat', 'dingtalk', 'feishu', 'serverchan', 'pushplus'] 任选
    默认推送到所有已配置通道
    rich_text: 是否使用富文本模式(None=读取配置)
    """
    import bot
    if channels is None:
        channels = ["wechat", "dingtalk", "feishu", "serverchan", "pushplus"]
    if rich_text is None:
        rich_text = bot.get_config("rich_text_mode", False)

    def _push_one(ch):
        """推送单个通道"""
        if ch == "wechat":
            ok, msg = (push_wechat_markdown(text) if rich_text else push_wechat(text))
        elif ch == "dingtalk":
            ok, msg = push_dingtalk(text)
        elif ch == "feishu":
            ok, msg = (push_feishu_rich(text) if rich_text else push_feishu(text))
        elif ch == "serverchan":
            ok, msg = push_serverchan(text)
        elif ch == "pushplus":
            ok, msg = push_pushplus(text)
        else:
            return ch, None
        if ok:
            log.info(f"[{ch}] 推送成功")
        else:
            log.warning(f"[{ch}] {msg}")
        return ch, {"success": ok, "message": msg}

    # 并行推送所有通道
    from concurrent.futures import ThreadPoolExecutor
    results = {}
    with ThreadPoolExecutor(max_workers=len(channels)) as executor:
        futures = {executor.submit(_push_one, ch): ch for ch in channels}
        for future in futures:
            ch, result = future.result()
            if result:
                results[ch] = result
    return results


# ==================== Webhook 连通性预检 ===================

def test_webhook(key, value, timeout=None):
    """测试单个 webhook 连通性，返回 (bool, message)"""
    import bot
    if timeout is None:
        timeout = bot.PUSH_TIMEOUT
    try:
        # 如果值为空或占位符，从已保存的凭证中读取
        if not value or value == 'configured':
            value = bot.get_credential(key, "")
            if not value:
                return False, f"未配置 {key}"
        if key == "wechat_webhook":
            url = value.strip().split("\n")[0].strip()
            resp = bot.get_http_session().post(url, json={"msgtype": "text", "text": {"content": "🔔 晚霞推送连通性测试"}}, timeout=timeout)
            if resp.status_code == 200 and resp.json().get("errcode") == 0:
                return True, "企业微信 Webhook 连通成功"
            return False, f"HTTP {resp.status_code}: {resp.text[:100]}"
        elif key == "dingtalk_webhook":
            resp = bot.get_http_session().post(value, json={"msgtype": "text", "text": {"content": "🔔 晚霞推送连通性测试"}}, timeout=timeout)
            if resp.status_code == 200:
                return True, "钉钉 Webhook 连通成功"
            return False, f"HTTP {resp.status_code}: {resp.text[:100]}"
        elif key == "feishu_webhook":
            resp = bot.get_http_session().post(value, json={"msg_type": "text", "content": {"text": "🔔 晚霞推送连通性测试"}}, timeout=timeout)
            if resp.status_code == 200:
                return True, "飞书 Webhook 连通成功"
            return False, f"HTTP {resp.status_code}: {resp.text[:100]}"
        elif key == "serverchan_sendkey":
            resp = bot.get_http_session().post(f"https://sctapi.ftqq.com/{value}.send", data={"title": "连通测试", "desp": "测试消息"}, timeout=timeout)
            if resp.status_code == 200 and resp.json().get("code") == 0:
                return True, "Server酱连通成功"
            return False, f"HTTP {resp.status_code}"
        elif key == "pushplus_token":
            resp = bot.get_http_session().post("https://www.pushplus.plus/send", json={"token": value, "title": "连通测试", "content": "测试"}, timeout=timeout)
            if resp.status_code == 200:
                d = resp.json()
                if d.get("code") == 200:
                    return True, "PushPlus 连通成功"
                return False, f"PushPlus 错误: {d.get('msg', '未知错误')} (code={d.get('code')})"
            return False, f"HTTP {resp.status_code}: {resp.text[:100]}"
        return False, f"不支持的 key: {key}"
    except requests.Timeout:
        return False, "连接超时"
    except Exception as e:
        return False, f"连接异常: {e}"
