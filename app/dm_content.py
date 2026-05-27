from __future__ import annotations

import html


def content_type_label(content_type: str | None) -> str:
    mapping = {
        "text": "文本",
        "media": "媒体",
        "forward": "频道转发",
    }
    return mapping.get(str(content_type or "text"), "文本")


def message_mode_label(message_mode: str | None, *, content_type: str | None = None) -> str:
    return "三段式" if str(message_mode or "single") == "three_stage" else "单条"


def payload_preview(payload: dict | None, *, content_type: str | None = None, max_len: int = 240) -> str:
    payload = payload or {}
    kind = str(content_type or payload.get("content_type") or "text")
    if kind == "media":
        media_kind = str(payload.get("media_kind") or "file")
        file_name = str(payload.get("file_name") or "未命名文件")
        caption = str(payload.get("caption") or "").strip()
        summary = f"{media_kind}｜{file_name}"
        if caption:
            summary += f"｜说明：{caption}"
        if str(payload.get("mode") or "single") == "three_stage":
            greeting = str(payload.get("greeting") or "").strip()[:60]
            closing = str(payload.get("closing") or "").strip()[:60]
            parts = []
            if greeting:
                parts.append(f"第1段：{greeting}")
            parts.append(f"第2段：{summary[:120]}")
            if closing:
                parts.append(f"第3段：{closing}")
            return html.escape("\n".join(parts)[:max_len], quote=False)
        return html.escape(summary[:max_len], quote=False)
    if kind == "forward":
        link = str(payload.get("forward_link") or "").strip()
        preview = str(payload.get("forward_preview") or "").strip()
        message_preview = str(payload.get("forward_message_preview") or "").strip()
        summary = f"频道帖子链接：{link or '-'}"
        if preview:
            summary += f"｜备注：{preview}"
        if message_preview:
            summary += f"｜帖子预览：{message_preview}"
        main_summary = summary[:max_len]
        if str(payload.get("mode") or "single") == "three_stage":
            greeting = str(payload.get("greeting") or "").strip()[:60]
            closing = str(payload.get("closing") or "").strip()[:60]
            parts = []
            if greeting:
                parts.append(f"第1段：{greeting}")
            parts.append(f"第2段：{main_summary}")
            if closing:
                parts.append(f"第3段：{closing}")
            return html.escape("\n".join(parts)[:max_len], quote=False)
        return html.escape(main_summary[:max_len], quote=False)
    mode = str(payload.get("mode") or "single")
    if mode == "three_stage":
        parts = [
            f"问候语：{str(payload.get('greeting') or '').strip()[:80]}",
            f"主消息：{str(payload.get('body') or '').strip()[:120]}",
            f"结束语：{str(payload.get('closing') or '').strip()[:80]}",
        ]
        return html.escape("\n".join(parts)[:max_len], quote=False)
    return html.escape(str(payload.get("text") or "")[:max_len], quote=False)
