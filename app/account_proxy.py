from __future__ import annotations

from typing import Any


PROXY_TYPE_LABELS = {
    "http": "HTTP",
    "socks5": "SOCKS5",
}


def _row_value(row: Any, key: str):
    if row is None:
        return None
    try:
        return row[key]
    except Exception:
        return getattr(row, key, None)


def parse_account_proxy_input(raw: str, proxy_type: str) -> dict[str, Any]:
    normalized_type = str(proxy_type or "").strip().lower()
    if normalized_type not in PROXY_TYPE_LABELS:
        raise ValueError("代理类型只支持 HTTP 或 SOCKS5")

    text = str(raw or "").strip()
    parts = [item.strip() for item in text.split(":")]
    if len(parts) != 4:
        raise ValueError("格式不对，请按 ip:端口:账号:密码 发送")

    host, port_text, username, password = parts
    if not host:
        raise ValueError("代理 IP 不能为空")
    if not port_text:
        raise ValueError("代理端口不能为空")
    if not username:
        raise ValueError("代理账号不能为空")
    if not password:
        raise ValueError("代理密码不能为空")

    try:
        port = int(port_text)
    except ValueError as exc:
        raise ValueError("代理端口必须是数字") from exc
    if port <= 0 or port > 65535:
        raise ValueError("代理端口请控制在 1 到 65535 之间")

    return {
        "proxy_type": normalized_type,
        "proxy_host": host,
        "proxy_port": port,
        "proxy_username": username,
        "proxy_password": password,
    }


def format_account_proxy_label(account_row: Any) -> str:
    proxy_type = str(_row_value(account_row, "proxy_type") or "").strip().lower()
    host = str(_row_value(account_row, "proxy_host") or "").strip()
    port = _row_value(account_row, "proxy_port")
    username = str(_row_value(account_row, "proxy_username") or "").strip()
    if not proxy_type or not host or not port:
        return "未设置"
    user_mask = "-"
    if username:
        user_mask = f"{username[:3]}***" if len(username) > 3 else f"{username}***"
    return f"{PROXY_TYPE_LABELS.get(proxy_type, proxy_type.upper())} {host}:{port} · {user_mask}"


def build_telethon_proxy(account_row: Any):
    proxy_type = str(_row_value(account_row, "proxy_type") or "").strip().lower()
    host = str(_row_value(account_row, "proxy_host") or "").strip()
    port = _row_value(account_row, "proxy_port")
    username = str(_row_value(account_row, "proxy_username") or "").strip() or None
    password = str(_row_value(account_row, "proxy_password") or "").strip() or None
    if not proxy_type or not host or not port:
        return None

    try:
        import socks
    except ImportError as exc:
        raise RuntimeError("当前环境缺少 PySocks，无法使用 HTTP / SOCKS5 代理") from exc

    proxy_kind = {
        "http": socks.HTTP,
        "socks5": socks.SOCKS5,
    }.get(proxy_type)
    if proxy_kind is None:
        raise ValueError("代理类型只支持 HTTP 或 SOCKS5")

    try:
        port_value = int(port)
    except Exception as exc:
        raise ValueError("代理端口无效") from exc

    return (proxy_kind, host, port_value, True, username, password)
