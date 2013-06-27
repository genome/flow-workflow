from flow.petri_net.success_failure_net import SuccessFailureNet

import logging


LOG = logging.getLogger(__name__)

class WorkflowNetBase(SuccessFailureNet):
    def __init__(self, name, operation_id, parent_operation_id=None):
        SuccessFailureNet.__init__(self, name=name)
        self.operation_id = operation_id
        self.parent_operation_id = parent_operation_id
