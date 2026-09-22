from types import SimpleNamespace
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, patch

from device_agent.commands.save_execution import perform_save
from device_agent.clients.container_maker_client import ContainerMakerClientError

_REAL_IMAGE = "registry.example.com/browseterm/user1/container1/img:1"


def _saved_pod_response() -> SimpleNamespace:
    return SimpleNamespace(saved_pods=[SimpleNamespace(pod_name="pod-1", namespace_name="user1-namespace")])


class TestPerformSave(IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.container_maker_client = AsyncMock()
        self.cloud_client = AsyncMock()
        patcher1 = patch("device_agent.commands.save_execution.config.SNAPSHOT_POLL_TIMEOUT_SECONDS", 0.05)
        patcher2 = patch("device_agent.commands.save_execution.config.SNAPSHOT_POLL_INTERVAL_SECONDS", 0.01)
        patcher1.start()
        patcher2.start()
        self.addCleanup(patcher1.stop)
        self.addCleanup(patcher2.stop)

    async def test_confirmed_success_returns_real_image(self) -> None:
        self.container_maker_client.save_container.return_value = _saved_pod_response()
        self.cloud_client.get_save_status.return_value = {"status": "Succeeded", "image_reference": _REAL_IMAGE}

        outcome = await perform_save(self.container_maker_client, self.cloud_client, "c1", "ns1", "req-1")

        self.assertTrue(outcome.succeeded)
        self.assertFalse(outcome.timed_out)
        self.assertEqual(outcome.image_reference, _REAL_IMAGE)
        self.assertEqual(outcome.pod_name, "pod-1")
        self.assertEqual(outcome.namespace, "user1-namespace")

    async def test_rpc_trigger_failure_never_polls(self) -> None:
        self.container_maker_client.save_container.side_effect = ContainerMakerClientError("snapshot job failed")

        outcome = await perform_save(self.container_maker_client, self.cloud_client, "c1", "ns1", "req-1")

        self.assertFalse(outcome.succeeded)
        self.assertFalse(outcome.timed_out)
        self.cloud_client.get_save_status.assert_not_awaited()

    async def test_confirmed_failure_is_not_success(self) -> None:
        '''The bug this whole module fixes: the RPC returning successfully must NOT be treated as
        the save having succeeded - only Cloud's confirmed report counts.'''
        self.container_maker_client.save_container.return_value = _saved_pod_response()
        self.cloud_client.get_save_status.return_value = {"status": "Failed", "error_detail": "push failed"}

        outcome = await perform_save(self.container_maker_client, self.cloud_client, "c1", "ns1", "req-1")

        self.assertFalse(outcome.succeeded)
        self.assertFalse(outcome.timed_out)
        self.assertEqual(outcome.error, "push failed")

    async def test_succeeded_status_without_image_reference_keeps_polling_then_times_out(self) -> None:
        '''A malformed/incomplete report (Succeeded but no image) must never be treated as a real
        success - better to time out than silently accept an unusable result.'''
        self.container_maker_client.save_container.return_value = _saved_pod_response()
        self.cloud_client.get_save_status.return_value = {"status": "Succeeded", "image_reference": None}

        outcome = await perform_save(self.container_maker_client, self.cloud_client, "c1", "ns1", "req-1")

        self.assertFalse(outcome.succeeded)
        self.assertTrue(outcome.timed_out)

    async def test_never_confirmed_times_out(self) -> None:
        self.container_maker_client.save_container.return_value = _saved_pod_response()
        self.cloud_client.get_save_status.return_value = {"status": "Running"}

        outcome = await perform_save(self.container_maker_client, self.cloud_client, "c1", "ns1", "req-1")

        self.assertFalse(outcome.succeeded)
        self.assertTrue(outcome.timed_out)
        self.assertEqual(outcome.pod_name, "pod-1")  # still surfaced for logging even on timeout

    async def test_cloud_unreachable_mid_poll_is_bounded_by_timeout_not_immediate_failure(self) -> None:
        '''A transient Cloud outage must not be treated as an immediate failure - perform_save
        keeps polling (bounded by the same deadline) rather than giving up on the first error,
        since a real save may still complete successfully once Cloud is reachable again.'''
        self.container_maker_client.save_container.return_value = _saved_pod_response()
        self.cloud_client.get_save_status.side_effect = Exception("connection refused")

        outcome = await perform_save(self.container_maker_client, self.cloud_client, "c1", "ns1", "req-1")

        self.assertFalse(outcome.succeeded)
        self.assertTrue(outcome.timed_out)
