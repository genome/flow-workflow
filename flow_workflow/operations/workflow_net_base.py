from flow.petri_net.success_failure_net import SuccessFailureNet

import logging


LOG = logging.getLogger(__name__)

class WorkflowNetBase(SuccessFailureNet):
    def __init__(self, name, operation_id, input_connections, output_properties, 
            resources, parent_operation_id=None):
        SuccessFailureNet.__init__(self, name=name)
        self.operation_id = operation_id
        self.parent_operation_id = parent_operation_id
        self.input_connections = input_connections
        self.output_properties = output_properties
        self.resources = resources
