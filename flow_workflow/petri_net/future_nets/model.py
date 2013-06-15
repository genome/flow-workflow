from flow.petri_net import future
from flow_workflow.petri_net.future_nets.base import GenomeNetBase
from flow_workflow.petri_net.actions import data


class GenomeModelNet(GenomeNetBase):
    def __init__(self, input_connections, **kwargs):
        GenomeNetBase.__init__(self, **kwargs)

        # XXX This may need its own special action
        self.start_transition.action = future.FutureAction(
                cls=data.StoreDataAction,  # XXX Should this be LoadDataAction?
                args={'input_connections': input_connections})
