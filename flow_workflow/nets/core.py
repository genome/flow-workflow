from flow_workflow.historian.action import WorkflowHistorianUpdateAction
from flow_workflow.nets.io import *

import flow.petri.netbuilder as nb


class GenomeEmptyNet(nb.EmptyNet):
    @property
    def operation_id(self):
        return self._operation_id

    def __init__(self, builder, name, operation_id, parent_operation_id,
            input_connections, queue=None, resources=None):

        nb.EmptyNet.__init__(self, builder, name)

        self._operation_id = operation_id
        self.parent_operation_id = parent_operation_id
        self.input_connections = input_connections
        self.queue = queue
        self.resources = resources

    def _update_action(self, **kwargs):
        status = kwargs.pop('status', None)
        info = {"id": self.operation_id,
                "name": self.name,
                "status": status,
                "parent_net_key": None,
                "parent_operation_id": self.parent_operation_id}

        optional_attrs = ['parent_net_key', 'peer_operation_id', 'parallel_index']
        for attr in optional_attrs:
            value = getattr(self, attr, None)
            if value is not None:
                info[attr] = value

        args = {"children_info": [info]}
        args.update(kwargs)

        return nb.ActionSpec(WorkflowHistorianUpdateAction, args=args)
