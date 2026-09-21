'''
Container Maker client (Part 7: "Call Container Maker through existing mTLS gRPC").

Ported from browseterm-server-local/src/containers/containers_service.py's three K8s-facing
methods (create_container_in_k8s/delete_container_in_k8s/save_container_in_k8s) - the mTLS
cert-loading + channel construction and the container-maker request/response shape are UNCHANGED
from that code (container-maker itself is not part of this migration - "Keep Container Maker
private and focused on Kubernetes operations"). What's different here: no more FastAPI
HTTPException wrapping (this runs headless, callers get plain exceptions), and the DB-facing
methods from that class (create_container_in_db, update_container, list_user_containers, ...)
are NOT ported - those are Cloud-only concerns now, Cloud already owns Postgres directly.
'''
import asyncio
from typing import Any, Dict, List, Optional

import grpc
from container_maker_spec.service_pb2_grpc import ContainerMakerAPIStub
from container_maker_spec.types_pb2 import (
    CreateContainerRequest as GRPCCreateContainerRequest,
    DeleteContainerRequest as GRPCDeleteContainerRequest,
    SaveContainerRequest as GRPCSaveContainerRequest,
    ContainerResponse as GRPCContainerResponse,
    DeleteContainerResponse as GRPCDeleteContainerResponse,
    ResourceRequirements as GRPCResourceRequirements,
    PublishInformation as GRPCPublishInformation,
)

from device_agent.observability.logging_setup import get_logger

logger = get_logger("container_maker_client")


class ContainerMakerClientError(Exception):
    pass


class ContainerMakerClient:
    '''One instance per Device Agent process (the mTLS channel is reused across all commands -
    unlike the old per-request ContainerService(), Device Agent lives for the process lifetime,
    so there is no "eager Secret read on every list-containers call" problem that motivated the
    old code's lazy _ensure_grpc_client - this simply builds the channel once at construction.'''

    def __init__(self, host: str, port: int, client_key: bytes, client_cert: bytes, ca_cert: bytes) -> None:
        credentials = grpc.ssl_channel_credentials(
            root_certificates=ca_cert, private_key=client_key, certificate_chain=client_cert,
        )
        self._channel = grpc.secure_channel(f"{host}:{port}", credentials)
        self._stub = ContainerMakerAPIStub(channel=self._channel)

    def close(self) -> None:
        self._channel.close()

    async def create_container(
        self, image_name: str, container_name: str, network_name: str, exposure_level: int,
        publish_information: List[Dict[str, Any]], environment_variables: Dict[str, str],
        cpu_request: str, cpu_limit: str, memory_request: str, memory_limit: str,
        ephemeral_request: str, ephemeral_limit: str, snapshot_size_limit: Optional[str],
        request_id: str = "",
    ) -> GRPCContainerResponse:
        request = GRPCCreateContainerRequest(
            image_name=image_name, container_name=container_name, network_name=network_name,
            exposure_level=exposure_level,
            publish_information=[
                GRPCPublishInformation(
                    publish_port=p.get("publish_port", 2222), target_port=p.get("target_port", 22),
                    protocol=p.get("protocol", "TCP"), node_port=p.get("node_port"),
                )
                for p in publish_information
            ],
            environment_variables=environment_variables,
            resource_requirements=GRPCResourceRequirements(
                cpu_request=cpu_request, cpu_limit=cpu_limit, memory_request=memory_request,
                memory_limit=memory_limit, ephemeral_request=ephemeral_request, ephemeral_limit=ephemeral_limit,
                snapshot_size_limit=snapshot_size_limit,
            ),
        )
        try:
            return await asyncio.to_thread(self._stub.createContainer, request, metadata=(("x-request-id", request_id),))
        except grpc.RpcError as e:
            raise ContainerMakerClientError(f"Error creating container in ContainerMaker: {e}") from e

    async def delete_container(self, container_id: str, network_name: str, request_id: str = "") -> GRPCDeleteContainerResponse:
        request = GRPCDeleteContainerRequest(container_id=container_id, network_name=network_name)
        try:
            return await asyncio.to_thread(self._stub.deleteContainer, request, metadata=(("x-request-id", request_id),))
        except grpc.RpcError as e:
            raise ContainerMakerClientError(f"Error deleting container in ContainerMaker: {e}") from e

    async def save_container(self, container_id: str, network_name: str, request_id: str = "") -> Any:
        '''container-maker blocks until the snapshot Job completes - callers must run this in the
        background and report progress separately (Part 10's SNAPSHOTTING/BUILDING_IMAGE/
        PUSHING_IMAGE stages), not treat this single RPC's return as instantaneous.'''
        request = GRPCSaveContainerRequest(container_id=container_id, network_name=network_name)
        try:
            return await asyncio.to_thread(self._stub.saveContainer, request, metadata=(("x-request-id", request_id),))
        except grpc.RpcError as e:
            raise ContainerMakerClientError(f"Error saving container in ContainerMaker: {e}") from e
