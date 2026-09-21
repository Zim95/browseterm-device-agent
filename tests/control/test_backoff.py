import random
from unittest import TestCase

from device_agent.control.backoff import backoff_seconds


class TestBackoff(TestCase):
    def test_follows_the_documented_schedule_without_jitter(self) -> None:
        '''Doc-required: "Retry 1s, 2s, 4s, 8s, 15s, then around 30s."'''
        zero_jitter_rng = random.Random()
        zero_jitter_rng.uniform = lambda a, b: 0.0
        expected = [1, 2, 4, 8, 15, 30]
        for attempt, expected_delay in enumerate(expected):
            self.assertEqual(backoff_seconds(attempt, rng=zero_jitter_rng), expected_delay)

    def test_caps_at_the_last_schedule_value_beyond_its_length(self) -> None:
        zero_jitter_rng = random.Random()
        zero_jitter_rng.uniform = lambda a, b: 0.0
        self.assertEqual(backoff_seconds(10, rng=zero_jitter_rng), 30)
        self.assertEqual(backoff_seconds(100, rng=zero_jitter_rng), 30)

    def test_jitter_stays_within_the_documented_fraction(self) -> None:
        rng = random.Random(42)
        for attempt in range(6):
            base = [1, 2, 4, 8, 15, 30][attempt]
            delay = backoff_seconds(attempt, rng=rng)
            self.assertGreaterEqual(delay, base * 0.8)
            self.assertLessEqual(delay, base * 1.2)

    def test_never_returns_negative(self) -> None:
        always_min_rng = random.Random()
        always_min_rng.uniform = lambda a, b: a  # maximal negative jitter
        self.assertGreaterEqual(backoff_seconds(0, rng=always_min_rng), 0.0)
