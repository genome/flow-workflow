from flow.petri_net.future import FutureAction
from flow_workflow.operations.workflow_net_base import WorkflowNetBase
from flow_workflow.operations.pass_thru_action import PassThruAction

import logging


LOG = logging.getLogger(__name__)

class PassThruNet(WorkflowNetBase):
    name = 'DEFINE IN SUBCLASSES'

    def __init__(self, input_connections, **kwargs):
        WorkflowNetBase.__init__(self, **kwargs)

        args = {
            "operation_id": self._action_arg_operation_id,
            "input_connections": input_connections,
        }

        self.store_transition = self.bridge_places(
                self.internal_start_place, self.internal_success_place,
                name=self.name + '(%s)' % self.operation_id)

        self.store_transition.action = FutureAction(
                cls=PassThruAction, args=args)

    @property
    def _action_arg_operation_id(self):
        self.operation_id
