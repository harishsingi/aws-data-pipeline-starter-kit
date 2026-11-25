"""Retry with exponential backoff for transient AWS calls (Lambda copy).

MIRRORED COPY: the canonical implementation lives in
``src/pipeline_kit/retry.py``. This copy exists because the Lambda
deployment zip (``lambda/src``) is self-contained. Keep the behaviour
identical; ``tests/test_mirrors.py`` asserts the mirrors agree.
"""

from __future__ import annotations

import functools
import logging
import random
import time
from dataclasses import dataclass
from typing import Callable, ParamSpec, TypeVar

logger = logging.getLogger(__name__)

P = ParamSpec("P")
T = TypeVar("T")


class RetryExhaustedError(Exception):
    """A retried operation failed on every attempt."""

    def __init__(self, message: str, *, attempts: int, last_error: BaseException | None = None) -> None:
        super().__init__(message)
        self.attempts = attempts
        self.last_error = last_error


@dataclass(frozen=True)
class RetryConfig:
    """Tunable retry policy. Defaults are conservative for S3/CloudWatch."""

    max_attempts: int = 3
    base_delay_seconds: float = 0.5
    max_delay_seconds: float = 8.0
    retryable_exceptions: tuple[type[BaseException], ...] = (Exception,)

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        if self.base_delay_seconds < 0 or self.max_delay_seconds < 0:
            raise ValueError("delays must be >= 0")
        if self.max_delay_seconds < self.base_delay_seconds:
            raise ValueError("max_delay_seconds must be >= base_delay_seconds")


def backoff_delay(attempt: int, *, base: float, cap: float,
                  rng: random.Random | None = None) -> float:
    """Exponential backoff with full jitter for a 0-based ``attempt``."""
    ceiling = min(cap, base * (2 ** attempt))
    return (rng or random).uniform(0, ceiling)


def retry_with_backoff(
    config: RetryConfig | None = None,
    *,
    sleep: Callable[[float], None] | None = None,
    rng: random.Random | None = None,
) -> Callable[[Callable[P, T]], Callable[P, T]]:
    """Decorator: retry a function on retryable exceptions."""
    policy = config or RetryConfig()
    _sleep = sleep or time.sleep

    def decorator(fn: Callable[P, T]) -> Callable[P, T]:
        @functools.wraps(fn)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            last_error: BaseException | None = None
            for attempt in range(policy.max_attempts):
                try:
                    return fn(*args, **kwargs)
                except policy.retryable_exceptions as exc:
                    last_error = exc
                    if attempt == policy.max_attempts - 1:
                        break
                    delay = backoff_delay(
                        attempt,
                        base=policy.base_delay_seconds,
                        cap=policy.max_delay_seconds,
                        rng=rng,
                    )
                    logger.warning(
                        "Attempt %d/%d of %s failed (%s); retrying in %.2fs",
                        attempt + 1,
                        policy.max_attempts,
                        fn.__name__,
                        exc,
                        delay,
                    )
                    _sleep(delay)
            raise RetryExhaustedError(
                f"{fn.__name__} failed after {policy.max_attempts} attempt(s)",
                attempts=policy.max_attempts,
                last_error=last_error,
            ) from last_error

        return wrapper

    return decorator
