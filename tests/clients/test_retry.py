from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, patch

from device_agent.clients.retry import call_with_retry


class TestCallWithRetry(IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        patcher = patch("device_agent.clients.retry.asyncio.sleep", new=AsyncMock())
        self.mock_sleep = patcher.start()
        self.addCleanup(patcher.stop)

    async def test_returns_on_first_success_without_sleeping(self) -> None:
        async def ok():
            return "ok"

        result = await call_with_retry(ok, is_retryable=lambda e: True)
        self.assertEqual(result, "ok")
        self.mock_sleep.assert_not_called()

    async def test_retries_a_retryable_failure_then_succeeds(self) -> None:
        attempts = {"n": 0}

        async def flaky():
            attempts["n"] += 1
            if attempts["n"] < 3:
                raise ValueError("transient")
            return "ok"

        result = await call_with_retry(flaky, is_retryable=lambda e: True, max_attempts=3)
        self.assertEqual(result, "ok")
        self.assertEqual(attempts["n"], 3)
        self.assertEqual(self.mock_sleep.call_count, 2)

    async def test_does_not_retry_a_non_retryable_failure(self) -> None:
        attempts = {"n": 0}

        async def always_fails():
            attempts["n"] += 1
            raise ValueError("permanent")

        with self.assertRaises(ValueError):
            await call_with_retry(always_fails, is_retryable=lambda e: False, max_attempts=3)
        self.assertEqual(attempts["n"], 1)
        self.mock_sleep.assert_not_called()

    async def test_raises_final_exception_after_exhausting_attempts(self) -> None:
        attempts = {"n": 0}

        async def always_fails():
            attempts["n"] += 1
            raise ValueError(f"attempt {attempts['n']}")

        with self.assertRaises(ValueError) as ctx:
            await call_with_retry(always_fails, is_retryable=lambda e: True, max_attempts=3)
        self.assertEqual(attempts["n"], 3)
        self.assertEqual(str(ctx.exception), "attempt 3")
