'''Small async retry helper for this repo's Cloud/Container Maker clients (todays_plan.md item 6:
"Add retries to requests if they fail"). These clients are async natively (httpx.AsyncClient,
grpc calls wrapped in asyncio.to_thread), unlike the workload repos' synchronous clients that
already got the equivalent helper (browseterm_workload/*/src/retry.py) - same idea, asyncio.sleep
instead of a blocking time.sleep.
'''
import asyncio
import random
from typing import Awaitable, Callable, TypeVar

T = TypeVar("T")


async def call_with_retry(
    fn: Callable[[], Awaitable[T]],
    is_retryable: Callable[[BaseException], bool],
    max_attempts: int = 3,
    base_delay_seconds: float = 0.5,
    max_delay_seconds: float = 5.0,
) -> T:
    '''Awaits fn() up to max_attempts times. Retries only exceptions is_retryable(e) accepts - an
    application-level rejection (a single-use ticket already consumed, a bad request) will not
    succeed on retry and must surface on the first attempt. Re-raises the final attempt's
    exception either way.'''
    for attempt in range(max_attempts):
        try:
            return await fn()
        except Exception as e:
            if attempt == max_attempts - 1 or not is_retryable(e):
                raise
            delay = min(max_delay_seconds, base_delay_seconds * (2 ** attempt))
            await asyncio.sleep(delay * random.uniform(0.5, 1.5))
    raise AssertionError("unreachable")  # pragma: no cover
