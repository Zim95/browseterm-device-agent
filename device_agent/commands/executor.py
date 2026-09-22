'''
Dispatches an accepted ExecuteCommand to its operation-specific handler, journals the result, and
reports it back to Cloud. Journaling happens BEFORE reporting (see CommandJournal's own docstring)
so a crash between "handler finished" and "Cloud acknowledged" always has a durable result to
resend on the next connection - this ordering is the actual mechanism behind Part 7's required
"crash after Container Maker succeeds but before Cloud acknowledgement" test.
'''
from typing import Awaitable, Callable, Dict, Optional

from device_agent.commands._util import operation_name
from device_agent.state.command_journal import CommandJournal
from device_agent.state.placement_cache import PlacementCache
from device_agent.observability.logging_setup import get_logger

logger = get_logger("command_executor")

# A handler receives the raw ExecuteCommand protobuf and returns (result_dict_or_None,
# error_code_or_None, error_message_or_None). Exactly one of (result) or (error_code,
# error_message) should be meaningful - a handler that raises is treated as a FAILED result with
# a generic error_code, never left unreported.
CommandHandler = Callable[["ExecuteCommand"], Awaitable[tuple]]

# Operations that place a container on this device - the placement_cache entry must exist (or be
# refreshed) once these succeed, so ReportContainerStatus (migration Part 12) can later supply the
# real placement_generation instead of trusting status_monitor's guess (see placement_cache.py).
_PLACING_OPERATIONS = frozenset({"Create", "Resume"})
# Operations that remove a container from this device - stale placement info must not linger.
_UNPLACING_OPERATIONS = frozenset({"Delete", "Hibernate"})


class CommandExecutor:
    def __init__(
        self, journal: CommandJournal, report_result: Callable[[str, str, Optional[dict], Optional[str], Optional[str]], Awaitable[None]],
        placement_cache: Optional[PlacementCache] = None,
    ) -> None:
        self.journal = journal
        self.report_result = report_result
        self.placement_cache = placement_cache
        self._handlers: Dict[int, CommandHandler] = {}

    def register(self, operation_enum: int, handler: CommandHandler) -> None:
        self._handlers[operation_enum] = handler

    async def execute(self, command_id: str, execute_command: "ExecuteCommand") -> None:
        handler = self._handlers.get(execute_command.operation)
        if handler is None:
            logger.error("command.no_handler_registered", extra={"command_id": command_id, "operation": execute_command.operation})
            await self._finish(command_id, "failed", None, "NO_HANDLER", f"No handler registered for operation {execute_command.operation}")
            return

        # Defensive: normally ConnectionManager._handle_inbound already called record_accepted
        # before this executor ever runs (that ordering is what makes has_seen()-based dedup
        # work at all) - but execute() must not silently no-op the journal writes below if a
        # caller ever invokes it directly without that happening first, or a result could be
        # computed and reported to Cloud while never becoming locally durable/dedup-able.
        if not self.journal.has_seen(command_id):
            self.journal.record_accepted(command_id, operation_name(execute_command.operation))
        self.journal.record_running(command_id)
        try:
            result, error_code, error_message = await handler(execute_command)
            if error_code is not None:
                await self._finish(command_id, "failed", None, error_code, error_message)
            else:
                self._update_placement_cache(execute_command)
                await self._finish(command_id, "succeeded", result, None, None)
        except Exception as e:
            logger.error("command.handler_raised", exc_info=True, extra={"command_id": command_id})
            await self._finish(command_id, "failed", None, "HANDLER_EXCEPTION", str(e)[:1000])

    def _update_placement_cache(self, execute_command: "ExecuteCommand") -> None:
        '''Only called after a handler succeeds. See _PLACING_OPERATIONS/_UNPLACING_OPERATIONS
        and placement_cache.py's own docstring for why this exists.'''
        if self.placement_cache is None:
            return
        op = operation_name(execute_command.operation)
        if op in _PLACING_OPERATIONS:
            self.placement_cache.record(execute_command.container_id, execute_command.device_id, execute_command.placement_generation)
        elif op in _UNPLACING_OPERATIONS:
            self.placement_cache.forget(execute_command.container_id)

    async def _finish(self, command_id: str, status: str, result: Optional[dict], error_code: Optional[str], error_message: Optional[str]) -> None:
        self.journal.record_result(command_id, status, result=result, error_code=error_code, error_message=error_message)
        # report_result only queues the CommandResult for send (see ConnectionManager._send) - it
        # does not wait for a transport-level ack, because CommandResult has none in this
        # protocol. There's a narrow residual window where the connection drops between the queue
        # accepting the message and it actually reaching the wire, in which case mark_reported
        # below fires slightly early. This is deliberately not engineered away here: a lost result
        # in that window becomes a "stuck transitional command" from Cloud's perspective, which
        # Part 22's reconciliation loop is explicitly designed to detect and redeliver/reconcile -
        # the same doc-mandated "duplicate delivery is expected and safe" property that makes
        # resending on every reconnect safe also makes this acceptable.
        await self.report_result(command_id, status, result, error_code, error_message)
        self.journal.mark_reported(command_id)
