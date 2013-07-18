from flow_workflow.future_nets import WorkflowNetBase


class PassThroughNet(WorkflowNetBase):
    def __init__(self, name, operation_id, input_connections,
            parent_operation_id=None):
        WorkflowNetBase.__init__(self, name=name,
                operation_id=operation_id,
                input_connections=input_connections,
                resources=None,
                parent_operation_id=parent_operation_id)

        self.skipping_place = self.bridge_transitions(
                self.internal_start_transition,
                self.internal_success_transition,
                name='skipping place')
