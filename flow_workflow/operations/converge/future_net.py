from flow.petri_net import future
from flow_workflow.petri_net.actions.converge import GenomeConvergeAction
from flow_workflow.petri_net.future_nets.base import GenomeNetBase


class GenomeConvergeNet(GenomeNetBase):
    def __init__(self, input_connections, input_property_order,
            output_properties, **kwargs):
        GenomeNetBase.__init__(self, **kwargs)

        args = {
            "operation_id": self.operation_id,
            "input_property_order": input_property_order,
            "output_properties": output_properties,
            "input_connections": input_connections,
        }

        self.converge_transition = self.bridge_places(
                self.internal_start_place, self.internal_success_place,
                name='converge(%s)' % self.operation_id)

        self.converge_transition.action = future.FutureAction(
                cls=GenomeConvergeAction, args=args)
