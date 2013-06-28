from flow.petri_net.future import FutureAction
from flow_workflow.operations.workflow_net_base import WorkflowNetBase
from flow_workflow.operations.pass_thru_action import PassThruAction

import logging


LOG = logging.getLogger(__name__)

class PassThruNet(WorkflowNetBase):
    _name = 'DEFINE IN SUBCLASSES'

    def __init__(self, operation_id, input_connections,
            output_properties, resources,
            parent_operation_id=None):
        WorkflowNetBase.__init__(self, name=self._name,
                operation_id=operation_id,
                input_connections=input_connections,
                output_properties=output_properties,
                resources=resources,
                parent_operation_id=parent_operation_id)

        args = {
            "operation_id": self._action_arg_operation_id,
            "input_connections": input_connections,
        }

        store_action = FutureAction(cls=PassThruAction, **args)

        self.store_transition = self.add_basic_transition(
                name=self.name + '(%s)' % self.operation_id,
                action=store_action)

        self.starting_place = self.bridge_transitions(
                self.internal_start_transition,
                self.store_transition,
                name='starting')

        self.succeeding_place = self.bridge_transitions(
                self.store_transition,
                self.internal_success_transition,
                name='succeeding')

    @property
    def _action_arg_operation_id(self):
        return self.operation_id
