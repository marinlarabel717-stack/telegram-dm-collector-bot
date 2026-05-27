from __future__ import annotations


def task_tag(task_id: int) -> str:
    return f"【私信任务{task_id}】"


def account_tag(account_id: int | None) -> str:
    if account_id is None:
        return ""
    return f"【账号{account_id}】"


def recipient_tag(value: str | None) -> str:
    if not value:
        return ""
    return f"【用户 {value}】"


def compose_log(message: str, *, task_id: int | None = None, account_id: int | None = None, recipient: str | None = None) -> str:
    prefix = "".join(
        chunk for chunk in (
            task_tag(task_id) if task_id is not None else "",
            account_tag(account_id),
            recipient_tag(recipient),
        ) if chunk
    )
    return f"{prefix} {message}".strip()
