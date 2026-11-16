"""A worker interceptor that sees, and can refuse, the child workflows a conversation starts."""

from temporalio.exceptions import WorkflowAlreadyStartedError
from temporalio.worker import (
    Interceptor,
    StartChildWorkflowInput,
    WorkflowInboundInterceptor,
    WorkflowInterceptorClassInput,
    WorkflowOutboundInterceptor,
)


class ChildStarts(Interceptor):
    """Records the ID of each child workflow start; refuses every start when ``refuse`` is set."""

    def __init__(self, refuse: bool = False) -> None:
        self.started: list[str] = []
        owner = self

        class Outbound(WorkflowOutboundInterceptor):
            async def start_child_workflow(self, input: StartChildWorkflowInput):
                owner.started.append(input.id)
                if refuse:
                    raise WorkflowAlreadyStartedError(input.id, input.workflow)
                return await super().start_child_workflow(input)

        class Inbound(WorkflowInboundInterceptor):
            def init(self, outbound: WorkflowOutboundInterceptor) -> None:
                super().init(Outbound(outbound))

        self._inbound = Inbound

    def workflow_interceptor_class(
        self, input: WorkflowInterceptorClassInput
    ) -> type[WorkflowInboundInterceptor]:
        return self._inbound
