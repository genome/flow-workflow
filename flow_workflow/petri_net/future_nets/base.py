from flow.petri_net.future_net import FutureNet
from flow.petri_net.success_failure_net import SuccessFailureNet


class GenomeNetBase(SuccessFailureNet):
    def __init__(self, name, operation_id, parent_operation_id=None):
        SuccessFailureNet.__init__(self, name=name)
        self.operation_id = operation_id
        self.parent_operation_id = parent_operation_id
