from flow_workflow.historian.action import WorkflowHistorianUpdateAction
from flow_workflow.nets.io import *

import flow.petri.netbuilder as nb


class GenomeEmptyNet(nb.EmptyNet):
    def __init__(self, builder, name, operation_id, parent_operation_id,
            input_connections, queue=None, resources=None):

        nb.EmptyNet.__init__(self, builder, name)

        self.operation_id = operation_id
        self.parent_operation_id = parent_operation_id
        self.input_connections = input_connections
        self.queue = queue
        self.resources = resources

    def _update_action(self, status):
        info = {"id": self.operation_id, "status": status, "name": self.name,
                "parent_net_key": None,
                "parent_operation_id": self.parent_operation_id}
        args = {"children_info": [info]}

        return nb.ActionSpec(WorkflowHistorianUpdateAction, args=args)
