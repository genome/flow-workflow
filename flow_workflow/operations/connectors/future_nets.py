from flow.petri_net.future import FutureAction
from flow_workflow.petri_net.actions import data
from flow_workflow.petri_net.future_nets.base import GenomeNetBase


class GenomeConnectorBase(GenomeNetBase):
    def __init__(self, input_connections, **kwargs):
        GenomeNetBase.__init__(self, **kwargs)

        args = {
            "operation_id": self._action_arg_operation_id,
            "input_connections": input_connections,
        }

        self.store_transition = self.bridge_places(
                self.internal_start_place, self.internal_success_place,
                name=self.name + '(%s)' % self.operation_id)

        self.store_transition.action = FutureAction(
                cls=data.StoreInputsAsOutputsAction, args=args)


class GenomeInputConnectorNet(GenomeConnectorBase):
    name = 'input-connector'

    @property
    def _action_arg_operation_id(self):
        self.operation_id


class GenomeOutputConnectorNet(GenomeConnectorBase):
    name = 'output-connector'

    @property
    def _action_arg_operation_id(self):
        self.parent.operation_id
