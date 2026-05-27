from __future__ import annotations

import logging

from .dm_logging import compose_log
from .dm_policy import DMTaskPolicy
from .dm_repository import DmRepository

logger = logging.getLogger(__name__)


class DmSenderManager:
    def __init__(self, repository: DmRepository):
        self.repository = repository

    async def run_task(self, task_id: int, policy: DMTaskPolicy | None = None) -> None:
        task = self.repository.get_dm_task(task_id)
        if not task:
            logger.warning(compose_log("任务不存在，无法启动", task_id=task_id))
            return
        active_policy = policy or DMTaskPolicy()
        logger.info(
            compose_log(
                f"发送器已准备接入｜模式={task['message_mode']}｜类型={task['content_type']}｜单号上限={active_policy.per_account_success_limit}",
                task_id=task_id,
            )
        )
