from flow_workflow.future_nets import WorkflowNetBase


class PassThroughNet(WorkflowNetBase):
    def __init__(self, operation_id, name):
        WorkflowNetBase.__init__(self, operation_id=operation_id, name=name)
        self.skipping_place = self.bridge_transitions(
                self.internal_start_transition,
                self.internal_success_transition,
                name='skipping place')
