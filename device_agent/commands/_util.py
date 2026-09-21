'''Shared helpers for command handlers.'''


def operation_name(operation_enum: int) -> str:
    '''Maps the wire CommandOperation enum to the plain string the journal/browseterm-db side
    stores (CommandOperation.CREATE.value etc). Single source of truth - both ConnectionManager
    and CommandExecutor need this mapping and must never drift apart on it.'''
    from device_control_spec.device_control_types_pb2 import (
        COMMAND_OPERATION_CREATE, COMMAND_OPERATION_DELETE, COMMAND_OPERATION_HIBERNATE,
        COMMAND_OPERATION_RESUME, COMMAND_OPERATION_RECONCILE,
    )
    mapping = {
        COMMAND_OPERATION_CREATE: "Create", COMMAND_OPERATION_DELETE: "Delete",
        COMMAND_OPERATION_HIBERNATE: "Hibernate", COMMAND_OPERATION_RESUME: "Resume",
        COMMAND_OPERATION_RECONCILE: "Reconcile",
    }
    return mapping.get(operation_enum, "Unknown")


def strip_container_maker_suffix(container_name: str) -> str:
    '''Format: mycontainer-pod-1706565890 -> mycontainer, mycontainer-service -> mycontainer.
    Verbatim port of browseterm-server-local's containers_service.py cleanup (lines 226-235).'''
    parts = container_name.split("-")
    if len(parts) >= 2 and parts[-1].isdigit():
        return "-".join(parts[:-2])
    elif len(parts) >= 1 and parts[-1] in ("pod", "service", "ingress"):
        return "-".join(parts[:-1])
    return container_name
