from flow.petri_net.future import FutureAction
from flow_workflow.petri_net.actions import data
from flow_workflow.petri_net.future_nets.base import GenomeNetBase


class GenomeInputConnectorNet(GenomeNetBase):
    def __init__(self, input_connections, **kwargs):
        GenomeNetBase.__init__(self, **kwargs)

        args = {
            "operation_id": self.operation_id,
            "input_connections": input_connections,
        }

        self.store_transition = self.bridge_places(
                self.internal_start_place, self.internal_success_place,
                name='input-connector(%s)' % self.operation_id)

        self.store_transition.action = FutureAction(
                cls=data.StoreDataAction, args=args)


class GenomeOutputConnectorNet(GenomeNetBase):
    def __init__(self, input_connections, **kwargs):
        GenomeNetBase.__init__(self, **kwargs)

        args = {
            "operation_id": self.operation_id,
            "input_connections": input_connections,
        }

        self.store_transition = self.bridge_places(
                self.internal_start_place, self.internal_success_place,
                name='output-connector-%s' % self.operation_id)

        # XXX This one is probably supposed to be LoadDataAction
        self.store_transition.action = FutureAction(
                cls=data.StoreDataAction, args=args)
