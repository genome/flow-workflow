from flow.petri_net.future import FutureAction
from flow_workflow.entities.converge.actions import ConvergeAction
from flow_workflow.future_nets import WorkflowNetBase
from flow_workflow.historian import actions


class ConvergeNet(WorkflowNetBase):
    def __init__(self, operation_id, name, input_property_order,
            output_properties):
        WorkflowNetBase.__init__(self, operation_id=operation_id, name=name)

        self.converge_action = FutureAction(cls=ConvergeAction,
                operation_id=operation_id,
                input_property_order=input_property_order,
                output_properties=output_properties)
        self.converge_transition = self.add_basic_transition(
                name='converge(%s)' % operation_id,
                action=self.converge_action)

        self.starting_place = self.bridge_transitions(
                self.internal_start_transition,
                self.converge_transition,
                name='starting')
        self.succeeding_place = self.bridge_transitions(
                self.converge_transition,
                self.internal_success_transition,
                name='succeeding')

        self.observe_transition(self.internal_start_transition,
                FutureAction(actions.UpdateOperationStatus,
                    operation_id=operation_id, status='running',
                    calculate_start_time=True))

        self.observe_transition(self.internal_success_transition,
                FutureAction(actions.UpdateOperationStatus,
                    operation_id=operation_id, status='done',
                    calculate_end_time=True))
