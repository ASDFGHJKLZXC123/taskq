from __future__ import annotations

import pytest

from taskq.backoff import BASE_DELAY_S, MAX_DELAY_S, MAX_SHIFT, compute_backoff


@pytest.mark.parametrize(
    "attempts,low,high",
    [
        (1, 2.0, 3.0),
        (2, 4.0, 6.0),
        (3, 8.0, 12.0),
        (4, 16.0, 24.0),
        (5, 32.0, 48.0),
    ],
)
def test_compute_backoff_windows(attempts: int, low: float, high: float) -> None:
    for _ in range(200):
        d = compute_backoff(attempts)
        assert low <= d <= high, f"attempts={attempts} produced {d}"


def test_compute_backoff_attempts_zero_window() -> None:
    for _ in range(200):
        d = compute_backoff(0)
        assert 1.0 <= d <= 1.5


def test_compute_backoff_jitter_non_negative() -> None:
    # No call should ever go below the deterministic backoff.
    for attempts in range(0, 12):
        det = BASE_DELAY_S * (2 ** min(attempts, MAX_SHIFT))
        for _ in range(50):
            d = compute_backoff(attempts)
            assert d >= min(det, MAX_DELAY_S)


def test_compute_backoff_caps_at_max_delay() -> None:
    # attempts >> MAX_SHIFT should still be capped at MAX_DELAY_S.
    for attempts in (MAX_SHIFT, MAX_SHIFT + 5, 100):
        for _ in range(100):
            d = compute_backoff(attempts)
            assert d <= MAX_DELAY_S
            assert d > 0.0
