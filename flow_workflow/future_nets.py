from flow.petri_net.success_failure_net import SuccessFailureNet


class WorkflowNetBase(SuccessFailureNet):
    def __init__(self, operation_id, name):
        SuccessFailureNet.__init__(self, name=name)
        self.operation_id = operation_id
