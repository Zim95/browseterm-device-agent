'''
Reconnect backoff (Part 7): "Retry 1s, 2s, 4s, 8s, 15s, then around 30s with jitter."
Pure function, no I/O - trivially testable and reused by both the control-stream reconnect loop
and (later) any other component that needs the identical schedule.
'''
import random

from device_agent.config import RECONNECT_BACKOFF_SCHEDULE_SECONDS, RECONNECT_JITTER_FRACTION


def backoff_seconds(attempt: int, rng: random.Random = None) -> float:
    '''attempt is 0-indexed (0 = first retry, after the initial connection attempt failed).
    Caps at the schedule's last value for every attempt beyond its length, jitter applied as
    +/- RECONNECT_JITTER_FRACTION of the base delay so many devices reconnecting at once don't
    all retry in lockstep.'''
    rng = rng or random
    schedule = RECONNECT_BACKOFF_SCHEDULE_SECONDS
    base = schedule[min(attempt, len(schedule) - 1)]
    jitter_range = base * RECONNECT_JITTER_FRACTION
    return max(0.0, base + rng.uniform(-jitter_range, jitter_range))
