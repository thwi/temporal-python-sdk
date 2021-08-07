import pytest

from temporal.workflow import signal_method, workflow_method, Workflow, WorkflowClient

TASK_QUEUE = "test_signal_with_start_workflow_tq"
NAMESPACE = "default"


class GreetingWorkflow:
    @signal_method
    async def hello(self) -> None:
        raise NotImplementedError

    @workflow_method(task_queue=TASK_QUEUE)
    async def get_greeting(self) -> None:
        raise NotImplementedError


class GreetingWorkflowImpl(GreetingWorkflow):
    workflow_method_executed: bool = False

    def __init__(self):
        self.signal_input = None

    async def hello(self, a, b):
        self.signal_input = [a, b]

    async def get_greeting(self, memo1, memo2):
        await Workflow.await_till(lambda: self.signal_input)
        return [ memo1, memo2, *self.signal_input ]


@pytest.mark.asyncio
@pytest.mark.worker_config(NAMESPACE, TASK_QUEUE, activities=[], workflows=[GreetingWorkflowImpl])
async def test(worker):
    client = WorkflowClient.new_client(namespace=NAMESPACE)

    start_payload_1 = { "name": "alpha" }
    start_payload_2 = { "name": "bravo" }
    signal_payload_1 = { "name": "charlie" }
    signal_payload_2 = { "name": "delta" }

    greeting_workflow: GreetingWorkflow = client.new_workflow_stub(GreetingWorkflow)
    context = await WorkflowClient.start(greeting_workflow.get_greeting, start_payload_1, start_payload_2,
                                         greeting_workflow.hello, signal_payload_1, signal_payload_2)
    ret_value = await client.wait_for_close(context)
    assert ret_value[0].get("name") == "alpha"
    assert ret_value[1].get("name") == "bravo"
    assert ret_value[2].get("name") == "charlie"
    assert ret_value[3].get("name") == "delta"
