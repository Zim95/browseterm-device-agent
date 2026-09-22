from unittest import TestCase

from device_agent.state.placement_cache import PlacementCache


class TestPlacementCache(TestCase):
    def setUp(self) -> None:
        self.cache = PlacementCache()

    def test_record_then_get_returns_device_and_generation(self) -> None:
        self.cache.record("c1", "d1", 3)
        self.assertEqual(self.cache.get("c1"), ("d1", 3))

    def test_get_missing_entry_returns_none(self) -> None:
        self.assertIsNone(self.cache.get("missing"))

    def test_record_overwrites_previous_entry_for_same_container(self) -> None:
        self.cache.record("c1", "d1", 1)
        self.cache.record("c1", "d1", 2)
        self.assertEqual(self.cache.get("c1"), ("d1", 2))

    def test_forget_removes_entry(self) -> None:
        self.cache.record("c1", "d1", 1)
        self.cache.forget("c1")
        self.assertIsNone(self.cache.get("c1"))

    def test_forget_missing_entry_is_a_no_op(self) -> None:
        self.cache.forget("never-recorded")  # must not raise

    def test_record_with_empty_container_id_is_ignored(self) -> None:
        self.cache.record("", "d1", 1)
        self.assertIsNone(self.cache.get(""))
