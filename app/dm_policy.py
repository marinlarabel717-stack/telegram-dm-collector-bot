from __future__ import annotations

import random
from dataclasses import dataclass, field


@dataclass(slots=True)
class DelayWindow:
    min_seconds: float = 8.0
    max_seconds: float = 15.0

    def next_delay(self) -> float:
        low = max(0.0, min(self.min_seconds, self.max_seconds))
        high = max(low, self.max_seconds)
        return random.uniform(low, high)


@dataclass(slots=True)
class RetryPolicy:
    max_retries: int = 3
    stop_account_after_user_frequent: int = 30
    stop_account_after_too_many_requests: int = 40


@dataclass(slots=True)
class DMTaskPolicy:
    per_account_success_limit: int = 40
    auto_switch_account: bool = True
    auto_stop_when_accounts_exhausted: bool = True
    typing_simulation: bool = True
    delay_window: DelayWindow = field(default_factory=DelayWindow)
    stage1_delay_seconds: float = 5.0
    stage2_delay_seconds: float = 3.0
    pin_after_send: bool = False
    pin_delay_seconds: float = 3.0
    delete_dialog_after_send: bool = False
    delete_dialog_delay_seconds: float = 0.0
    retry_policy: RetryPolicy = field(default_factory=RetryPolicy)

    def should_rotate_account(self, success_count: int) -> bool:
        return self.per_account_success_limit > 0 and success_count >= self.per_account_success_limit

    def should_stop_account_for_frequent(self, frequent_errors: int) -> bool:
        return frequent_errors >= self.retry_policy.stop_account_after_user_frequent

    def should_stop_account_for_too_many_requests(self, too_many_requests_hits: int) -> bool:
        return (
            self.retry_policy.stop_account_after_too_many_requests > 0
            and too_many_requests_hits >= self.retry_policy.stop_account_after_too_many_requests
        )
