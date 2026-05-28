from __future__ import annotations

import socket
from typing import Any
from urllib.parse import urlparse


PROXY_TYPE_LABELS = {
    "http": "HTTP",
    "socks5": "SOCKS5",
}

DEFAULT_PROXY_TEST_HOST = "149.154.167.51"
DEFAULT_PROXY_TEST_PORT = 443
DEFAULT_PROXY_TEST_TIMEOUT = 8.0


def _row_value(row: Any, key: str):
    if row is None:
        return None
    try:
        return row[key]
    except Exception:
        return getattr(row, key, None)


def normalize_proxy_type(proxy_type: str | None) -> str:
    normalized_type = str(proxy_type or "").strip().lower()
    if normalized_type in {"socks5", "sock5", "scok5", "socks"}:
        return "socks5"
    if normalized_type in {"http", "https"}:
        return "http"
    raise ValueError("代理类型只支持 HTTP 或 SOCKS5")


def _parse_proxy_parts(raw: str) -> tuple[str | None, str, int, str, str]:
    text = str(raw or "").strip()
    if not text:
        raise ValueError("代理内容不能为空")

    if "://" in text:
        parsed = urlparse(text)
        proxy_type = normalize_proxy_type(parsed.scheme)
        host = parsed.hostname or ""
        port = int(parsed.port or 0)
        username = parsed.username or ""
        password = parsed.password or ""
    else:
        parts = [item.strip() for item in text.split(":")]
        proxy_type = None
        if len(parts) == 5:
            proxy_type = normalize_proxy_type(parts[0])
            _, host, port_text, username, password = parts
        elif len(parts) == 4:
            host, port_text, username, password = parts
        else:
            raise ValueError("格式不对，请按 ip:端口:账号:密码 发送，或带 http:// / socks5:// 前缀")
        try:
            port = int(port_text)
        except ValueError as exc:
            raise ValueError("代理端口必须是数字") from exc

    if not host:
        raise ValueError("代理 IP 不能为空")
    if port <= 0 or port > 65535:
        raise ValueError("代理端口请控制在 1 到 65535 之间")
    if not username:
        raise ValueError("代理账号不能为空")
    if not password:
        raise ValueError("代理密码不能为空")
    return proxy_type, host, port, username, password


def build_proxy_candidates(raw: str) -> list[dict[str, Any]]:
    proxy_type, host, port, username, password = _parse_proxy_parts(raw)
    proxy_types = [proxy_type] if proxy_type else ["http", "socks5"]
    return [
        {
            "proxy_type": normalize_proxy_type(item),
            "proxy_host": host,
            "proxy_port": port,
            "proxy_username": username,
            "proxy_password": password,
        }
        for item in proxy_types
    ]


def parse_proxy_lines(raw: str) -> list[str]:
    lines: list[str] = []
    for line in str(raw or "").splitlines():
        cleaned = line.strip()
        if not cleaned or cleaned.startswith("#"):
            continue
        lines.append(cleaned)
    return lines


def format_account_proxy_label(proxy_row: Any) -> str:
    proxy_type = str(_row_value(proxy_row, "proxy_type") or "").strip().lower()
    host = str(_row_value(proxy_row, "proxy_host") or "").strip()
    port = _row_value(proxy_row, "proxy_port")
    username = str(_row_value(proxy_row, "proxy_username") or "").strip()
    if not proxy_type or not host or not port:
        return "未设置"
    proxy_type = normalize_proxy_type(proxy_type)
    user_mask = "-"
    if username:
        user_mask = f"{username[:3]}***" if len(username) > 3 else f"{username}***"
    return f"{PROXY_TYPE_LABELS.get(proxy_type, proxy_type.upper())} {host}:{port} · {user_mask}"


def summarize_proxy_pool(proxy_rows: list[dict[str, Any]] | None, *, max_items: int = 3) -> str:
    rows = proxy_rows or []
    if not rows:
        return "未设置"
    preview = "；".join(format_account_proxy_label(item) for item in rows[:max_items])
    extra = ""
    if len(rows) > max_items:
        extra = f"；另有 {len(rows) - max_items} 条"
    return f"共 {len(rows)} 条｜{preview}{extra}"


def build_telethon_proxy(proxy_row: Any):
    proxy_type = str(_row_value(proxy_row, "proxy_type") or "").strip().lower()
    host = str(_row_value(proxy_row, "proxy_host") or "").strip()
    port = _row_value(proxy_row, "proxy_port")
    username = str(_row_value(proxy_row, "proxy_username") or "").strip() or None
    password = str(_row_value(proxy_row, "proxy_password") or "").strip() or None
    if not proxy_type or not host or not port:
        return None

    try:
        import socks
    except ImportError as exc:
        raise RuntimeError("当前环境缺少 PySocks，无法使用 HTTP / SOCKS5 代理") from exc

    proxy_type = normalize_proxy_type(proxy_type)
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


def check_proxy_available(
    proxy_row: dict[str, Any],
    *,
    host: str = DEFAULT_PROXY_TEST_HOST,
    port: int = DEFAULT_PROXY_TEST_PORT,
    timeout: float = DEFAULT_PROXY_TEST_TIMEOUT,
) -> bool:
    try:
        import socks
    except ImportError as exc:
        raise RuntimeError("当前环境缺少 PySocks，无法检测 HTTP / SOCKS5 代理") from exc

    proxy = build_telethon_proxy(proxy_row)
    if not proxy:
        return False

    proxy_kind, proxy_host, proxy_port, rdns, username, password = proxy
    sock = socks.socksocket()
    sock.settimeout(timeout)
    try:
        sock.set_proxy(proxy_kind, proxy_host, int(proxy_port), rdns, username, password)
        sock.connect((host, int(port)))
        return True
    except OSError:
        return False
    finally:
        try:
            sock.close()
        except Exception:
            pass


def resolve_working_proxy(raw: str) -> dict[str, Any]:
    candidates = build_proxy_candidates(raw)
    last_candidate = candidates[-1]
    for candidate in candidates:
        if check_proxy_available(candidate):
            return candidate
    raise ValueError(
        f"代理不可用，已尝试：{' / '.join(PROXY_TYPE_LABELS[item['proxy_type']] for item in candidates)}"
    )
