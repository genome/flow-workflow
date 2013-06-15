from flow.petri_net import future
from flow_workflow.petri_net.future_nets.base import GenomeNetBase
from flow_workflow.petri_net.actions import data


class GenomeBlockNet(GenomeNetBase):
    def __init__(self, input_connections, **kwargs):
        GenomeNetBase.__init__(self, **kwargs)
        args = {
            'operation_id': self.operation_id,
            'input_connections': input_connections,
        }

        self.block_transition = self.bridge_places(
                self.internal_start_place, self.internal_success_place,
                name='block-%s' % self.operation_id)
        self.block_transition.action = future.FutureAction(
                cls=data.StoreDataAction, args=args)
