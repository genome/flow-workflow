from flow_workflow.nets.core import GenomeEmptyNet
from flow_workflow.nets.io import StoreOutputsAction
from flow_workflow.nets.io import StoreInputsAsOutputsAction

import flow.petri.netbuilder as nb


class GenomeInputConnectorNet(GenomeEmptyNet):
    def __init__(self, builder, name, operation_id, parent_operation_id):

        GenomeEmptyNet.__init__(self, builder, name, operation_id,
                parent_operation_id, input_connections=None)

        args = {"operation_id": operation_id}

        action = nb.ActionSpec(cls=StoreOutputsAction, args=args)
        self.start_transition = self.add_transition("input connector start",
                action=action
                )

        update_action = self._update_action(status="done",
                timestamps=["start_time", "end_time"])
        self.success_transition = self.add_transition("success",
                action=update_action)

        self.bridge_transitions(self.start_transition, self.success_transition)


class GenomeOutputConnectorNet(GenomeEmptyNet):
    def __init__(self, builder, name, operation_id, parent_operation_id,
            input_connections, workflow_id):

        GenomeEmptyNet.__init__(self, builder, name, operation_id,
                parent_operation_id, input_connections, queue=None,
                resources=None)

        self.workflow_id = workflow_id

        net = builder.add_subnet(nb.EmptyNet, self.name)

        args = {"operation_id": self.workflow_id,
                "input_connections": input_connections}
        action = nb.ActionSpec(cls=StoreInputsAsOutputsAction, args=args)

        self.start_transition = self.add_transition("output connector start",
                action=action)

        update_action = self._update_action(status="done",
                timestamps=["start_time", "end_time"])
        self.success_transition = self.add_transition("success",
                action=update_action)

        self.bridge_transitions(self.start_transition, self.success_transition)
