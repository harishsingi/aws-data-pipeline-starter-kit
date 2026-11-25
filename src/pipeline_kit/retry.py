"""Retry with exponential backoff for transient AWS / network calls.

Stdlib only. The decorator retries only the exception types the caller
names (default: :class:`Exception` is deliberately *not* the default --
callers must opt in to what is retryable), sleeps with exponential backoff
plus full jitter, and raises :class:`RetryExhaustedError` when the attempts
run out, chaining the last error.

The Lambda (``lambda/src/retry.py``) and Glue (``glue/src/etl/retry.py``)
packages carry a mirrored copy because their deployment zips are
self-contained; ``tests/test_mirrors.py`` asserts the mirrors behave the
same on a shared scenario.
"""

from __future__ import annotations

import functools
import logging
import random
import time
from dataclasses import dataclass
from typing import Callable, ParamSpec, TypeVar

from pipeline_kit.errors import RetryExhaustedError

logger = logging.getLogger(__name__)

P = ParamSpec("P")
T = TypeVar("T")


@dataclass(frozen=True)
class RetryConfig:
    """Tunable retry policy. Defaults are conservative for S3/CloudWatch."""

    max_attempts: int = 3
    base_delay_seconds: float = 0.5
    max_delay_seconds: float = 8.0
    # Exception types that are worth retrying (transient). Anything else
    # propagates immediately -- retrying a validation error is pointless.
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
    """Exponential backoff with full jitter for a 0-based ``attempt``.

    ``min(cap, base * 2**attempt)`` spread uniformly over ``[0, delay]``.
    Pure function, so tests can assert the bounds without sleeping.
    """
    ceiling = min(cap, base * (2 ** attempt))
    jitter = (rng or random).uniform(0, ceiling)
    return jitter


def retry_with_backoff(
    config: RetryConfig | None = None,
    *,
    sleep: Callable[[float], None] | None = None,
    rng: random.Random | None = None,
) -> Callable[[Callable[P, T]], Callable[P, T]]:
    """Decorator: retry a function on retryable exceptions.

    Parameters:
        config: Retry policy (defaults to :class:`RetryConfig()`).
        sleep: Sleep function (defaults to :func:`time.sleep`); tests inject
            a recorder so no real waiting happens.
        rng: Random source for jitter (tests inject a seeded one).
    """
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
