"""Dependency-free adaptive concurrency control."""

from __future__ import annotations

from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class Adaptation:
    previous: int
    current: int
    reason: str
    window_mib_s: float

    def as_dict(self) -> dict[str, int | float | str]:
        return asdict(self)


class AdaptiveConcurrency:
    """A conservative additive-increase/multiplicative-decrease controller.

    The controller only observes completed parts. It increases concurrency when
    aggregate throughput is still improving and halves it after a retry signal.
    A meaningful throughput regression reduces concurrency by one worker.
    """

    def __init__(
        self,
        *,
        initial: int,
        maximum: int,
        window_parts: int = 2,
        regression_threshold: float = 0.15,
        started_at: float = 0.0,
    ) -> None:
        if not 1 <= initial <= maximum:
            raise ValueError("adaptive concurrency must satisfy 1 <= initial <= maximum")
        if window_parts < 1:
            raise ValueError("window_parts must be greater than zero")
        if not 0 < regression_threshold < 1:
            raise ValueError("regression_threshold must be between zero and one")
        self.initial = initial
        self.current = initial
        self.maximum = maximum
        self.peak = initial
        self.window_parts = window_parts
        self.regression_threshold = regression_threshold
        self._window_started = started_at
        self._window_bytes = 0
        self._window_completions = 0
        self._previous_rate: float | None = None
        self.history: list[Adaptation] = []

    def observe(
        self,
        *,
        byte_count: int,
        completed_at: float,
        attempts: int,
    ) -> Adaptation | None:
        self._window_bytes += byte_count
        self._window_completions += 1

        if attempts > 1 and self.current > 1:
            elapsed = max(completed_at - self._window_started, 1e-9)
            rate = self._window_bytes / 1024**2 / elapsed
            return self._change(max(1, self.current // 2), "retry_backoff", rate, completed_at)

        if self._window_completions < self.window_parts:
            return None

        elapsed = max(completed_at - self._window_started, 1e-9)
        rate = self._window_bytes / 1024**2 / elapsed
        previous = self.current
        reason = "steady"
        if self._previous_rate is None or rate >= self._previous_rate:
            candidate = min(self.maximum, self.current + 1)
            reason = "throughput_probe" if candidate != self.current else "at_ceiling"
        elif rate < self._previous_rate * (1 - self.regression_threshold):
            candidate = max(1, self.current - 1)
            reason = "throughput_regression"
        else:
            candidate = self.current

        self._previous_rate = rate
        self._reset_window(completed_at)
        if candidate == previous:
            return None
        adaptation = Adaptation(previous, candidate, reason, rate)
        self.current = candidate
        self.peak = max(self.peak, candidate)
        self.history.append(adaptation)
        return adaptation

    def _change(
        self,
        candidate: int,
        reason: str,
        rate: float,
        completed_at: float,
    ) -> Adaptation | None:
        previous = self.current
        self._previous_rate = rate
        self._reset_window(completed_at)
        if candidate == previous:
            return None
        adaptation = Adaptation(previous, candidate, reason, rate)
        self.current = candidate
        self.history.append(adaptation)
        return adaptation

    def _reset_window(self, now: float) -> None:
        self._window_started = now
        self._window_bytes = 0
        self._window_completions = 0

