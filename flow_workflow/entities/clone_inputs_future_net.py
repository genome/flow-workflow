from flow.petri_net.future import FutureAction
from flow_workflow.entities.future_nets import WorkflowNetBase
from flow_workflow.entities.clone_inputs_action import CloneInputsAction


class CloneInputsNet(WorkflowNetBase):
    def __init__(self, name, operation_id, input_connections,
            parent_operation_id=None):
        WorkflowNetBase.__init__(self, name=name,
                operation_id=operation_id,
                input_connections=input_connections,
                resources=None,
                parent_operation_id=parent_operation_id)

        self.store_action = FutureAction(CloneInputsAction,
                operation_id=operation_id,
                input_connections=input_connections)
        self.store_transition = self.add_basic_transition(
                name=name + '(%s)' % operation_id,
                action=self.store_action)

        self.starting_place = self.bridge_transitions(
                self.internal_start_transition,
                self.store_transition,
                name='starting')

        self.succeeding_place = self.bridge_transitions(
                self.store_transition,
                self.internal_success_transition,
                name='succeeding')
