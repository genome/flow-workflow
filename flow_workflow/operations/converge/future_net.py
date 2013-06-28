from flow.petri_net.future import FutureAction
from flow_workflow.operations.converge.action import ConvergeAction
from flow_workflow.operations.workflow_net_base import WorkflowNetBase


class ConvergeNet(WorkflowNetBase):
    def __init__(self, name, operation_id, input_connections,
            input_property_order, output_properties,
            resources, parent_operation_id=None):
        WorkflowNetBase.__init__(self, name=name,
                operation_id=operation_id,
                input_connections=input_connections,
                output_properties=output_properties,
                resources=resources,
                parent_operation_id=parent_operation_id)

        args = {
            "operation_id": self.operation_id,
            "input_property_order": input_property_order,
            "output_properties": output_properties,
            "input_connections": input_connections,
        }

        action = FutureAction(cls=ConvergeAction, **args)
        self.converge_transition = self.add_basic_transition(
                name='converge(%s)' % self.operation_id,
                action=action)

        self.starting_place = self.bridge_transitions(
                self.internal_start_transition,
                self.converge_transition,
                name='starting')
        self.succeeding_place = self.bridge_transitions(
                self.converge_transition,
                self.internal_success_transition,
                name='succeeding')

