from flow.petri_net.success_failure_net import SuccessFailureNet


class WorkflowNetBase(SuccessFailureNet):
    def __init__(self, name, operation_id, input_connections,
            resources, parent_operation_id=None):
        SuccessFailureNet.__init__(self, name=name)

        self.input_connections = input_connections
        self.operation_id = operation_id
        self.parent_operation_id = parent_operation_id
        self.resources = resources
