'''
Device Agent entrypoint (Part 7). Reads device identity, wires the control-stream
ConnectionManager to the CommandExecutor, and runs forever.

Command handlers (create/delete/hibernate/resume) are registered here but implemented in
device_agent/commands/{create,delete,hibernate,resume}.py (Parts 8-11) - this file only wires
the pieces together, it contains no lifecycle business logic itself.
'''
import asyncio
import sys

from device_agent.observability.logging_setup import configure_logging, get_logger
configure_logging("browseterm-device-agent")
logger = get_logger("main")

import grpc
from device_control_spec import local_device_agent_pb2_grpc

from device_agent import config
from device_agent.control.grpc_client import ConnectionManager
from device_agent.commands.executor import CommandExecutor
from device_agent.commands import create as create_handler
from device_agent.commands import delete as delete_handler
from device_agent.commands import hibernate as hibernate_handler
from device_agent.commands import resume as resume_handler
from device_agent.commands import save as save_handler
from device_agent.clients.container_maker_client import ContainerMakerClient
from device_agent.clients.k8s_secrets import read_cert_from_k8s_secret
from device_agent.clients.cloud_client import CloudClient
from device_agent.local_api.service import LocalDeviceAgentServicer
from device_agent.state.command_journal import CommandJournal
from device_agent.state.placement_cache import PlacementCache
from device_control_spec.device_control_types_pb2 import (
    COMMAND_OPERATION_CREATE, COMMAND_OPERATION_DELETE, COMMAND_OPERATION_HIBERNATE, COMMAND_OPERATION_RESUME,
    COMMAND_OPERATION_SAVE,
)


def _read_required_file(path: str) -> str:
    try:
        with open(path, "r") as f:
            return f.read().strip()
    except FileNotFoundError:
        logger.error("main.missing_required_file", extra={"path": path})
        sys.exit(1)


def _build_container_maker_client() -> ContainerMakerClient:
    client_key = read_cert_from_k8s_secret(config.CONTAINER_MAKER_CERTS_SECRET_NAME, config.NAMESPACE, "client.key")
    client_cert = read_cert_from_k8s_secret(config.CONTAINER_MAKER_CERTS_SECRET_NAME, config.NAMESPACE, "client.crt")
    ca_cert = read_cert_from_k8s_secret(config.CONTAINER_MAKER_CERTS_SECRET_NAME, config.NAMESPACE, "ca.crt")
    return ContainerMakerClient(
        host=config.CONTAINER_MAKER_HOST, port=config.CONTAINER_MAKER_PORT,
        client_key=client_key, client_cert=client_cert, ca_cert=ca_cert,
    )


async def run() -> None:
    device_id = _read_required_file(config.DEVICE_ID_FILE)
    device_token = _read_required_file(config.DEVICE_TOKEN_FILE)
    try:
        startup_id = _read_required_file(config.STARTUP_ID_FILE)
    except SystemExit:
        startup_id = ""  # optional in local/dev environments without a real supervisor

    journal = CommandJournal(config.COMMAND_JOURNAL_PATH)
    placement_cache = PlacementCache()
    container_maker_client = _build_container_maker_client()
    cloud_client = CloudClient(device_id=device_id, device_token=device_token)

    connection_manager: ConnectionManager = None  # set below, referenced by the report_result closure

    async def report_result(command_id, status, result, error_code, error_message):
        await connection_manager.send_command_result(command_id, status, result, error_code, error_message)

    executor = CommandExecutor(journal=journal, report_result=report_result, placement_cache=placement_cache)
    executor.register(COMMAND_OPERATION_CREATE, create_handler.make_handler(container_maker_client, cloud_client))
    executor.register(COMMAND_OPERATION_DELETE, delete_handler.make_handler(container_maker_client, cloud_client))
    executor.register(COMMAND_OPERATION_HIBERNATE, hibernate_handler.make_handler(container_maker_client, cloud_client))
    executor.register(COMMAND_OPERATION_RESUME, resume_handler.make_handler(container_maker_client, cloud_client))
    executor.register(COMMAND_OPERATION_SAVE, save_handler.make_handler(container_maker_client, cloud_client))

    async def on_execute_command(command_id, execute_command):
        await executor.execute(command_id, execute_command)

    connection_manager = ConnectionManager(
        device_id=device_id, device_token=device_token, journal=journal,
        on_execute_command=on_execute_command, startup_id=startup_id,
    )

    local_api_server = grpc.aio.server()
    local_device_agent_pb2_grpc.add_LocalDeviceAgentServicer_to_server(
        LocalDeviceAgentServicer(connection_manager, cloud_client, placement_cache), local_api_server,
    )
    local_api_server.add_insecure_port(f"[::]:{config.LOCAL_API_PORT}")  # ClusterIP-only, NetworkPolicy-gated, not TLS

    logger.info("main.starting", extra={"device_id": device_id})
    await local_api_server.start()
    try:
        await connection_manager.run_forever()
    finally:
        await local_api_server.stop(5)
        container_maker_client.close()
        await cloud_client.close()


if __name__ == "__main__":
    asyncio.run(run())
