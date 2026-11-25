"""Tests for pipeline_kit.retry (stdlib unittest only)."""

import os
import random
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))

from pipeline_kit.errors import RetryExhaustedError  # noqa: E402
from pipeline_kit.retry import RetryConfig, backoff_delay, retry_with_backoff  # noqa: E402


class TestBackoffDelay(unittest.TestCase):
    def test_within_bounds(self):
        rng = random.Random(7)
        for attempt in range(6):
            delay = backoff_delay(attempt, base=0.5, cap=8.0, rng=rng)
            self.assertGreaterEqual(delay, 0.0)
            self.assertLessEqual(delay, min(8.0, 0.5 * (2 ** attempt)))

    def test_zero_base_is_zero(self):
        self.assertEqual(backoff_delay(3, base=0.0, cap=8.0, rng=random.Random(1)), 0.0)


class TestRetryConfig(unittest.TestCase):
    def test_invalid(self):
        with self.assertRaises(ValueError):
            RetryConfig(max_attempts=0)
        with self.assertRaises(ValueError):
            RetryConfig(base_delay_seconds=-1)
        with self.assertRaises(ValueError):
            RetryConfig(base_delay_seconds=5, max_delay_seconds=1)


class TestRetryWithBackoff(unittest.TestCase):
    def test_succeeds_first_try_no_sleep(self):
        sleeps: list[float] = []
        calls = []

        @retry_with_backoff(sleep=sleeps.append, rng=random.Random(1))
        def fn():
            calls.append(1)
            return "ok"

        self.assertEqual(fn(), "ok")
        self.assertEqual(len(calls), 1)
        self.assertEqual(sleeps, [])

    def test_retries_then_succeeds(self):
        sleeps: list[float] = []
        attempts = {"n": 0}

        @retry_with_backoff(
            RetryConfig(max_attempts=3, base_delay_seconds=0.1, max_delay_seconds=1.0),
            sleep=sleeps.append,
            rng=random.Random(2),
        )
        def fn():
            attempts["n"] += 1
            if attempts["n"] < 3:
                raise ConnectionError("flaky")
            return "ok"

        self.assertEqual(fn(), "ok")
        self.assertEqual(attempts["n"], 3)
        self.assertEqual(len(sleeps), 2)

    def test_exhausts_and_raises_retry_exhausted(self):
        sleeps: list[float] = []

        @retry_with_backoff(
            RetryConfig(max_attempts=2, retryable_exceptions=(ConnectionError,)),
            sleep=sleeps.append,
            rng=random.Random(3),
        )
        def fn():
            raise ConnectionError("down")

        with self.assertRaises(RetryExhaustedError) as ctx:
            fn()
        self.assertEqual(ctx.exception.attempts, 2)
        self.assertIsInstance(ctx.exception.last_error, ConnectionError)
        self.assertEqual(len(sleeps), 1)

    def test_non_retryable_exception_propagates_immediately(self):
        sleeps: list[float] = []
        calls = []

        @retry_with_backoff(
            RetryConfig(max_attempts=3, retryable_exceptions=(ConnectionError,)),
            sleep=sleeps.append,
            rng=random.Random(4),
        )
        def fn():
            calls.append(1)
            raise ValueError("not retryable")

        with self.assertRaises(ValueError):
            fn()
        self.assertEqual(len(calls), 1)
        self.assertEqual(sleeps, [])


if __name__ == "__main__":
    unittest.main()
