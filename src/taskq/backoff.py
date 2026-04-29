from __future__ import annotations

import random

BASE_DELAY_S = 1.0
MAX_DELAY_S = 600.0
MAX_SHIFT = 10


def compute_backoff(attempts: int) -> float:
    backoff = BASE_DELAY_S * (2 ** min(attempts, MAX_SHIFT))
    jitter = random.uniform(0, backoff / 2)
    return min(backoff + jitter, MAX_DELAY_S)
