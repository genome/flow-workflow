from flow.petri_net.future_net import FutureNet
from flow.petri_net.success_failure_net import SuccessFailureNet


class SimplifiedSuccessFailureNet(FutureNet):
    def __init__(self, name=''):
        FutureNet.__init__(self, name=name)

        # Internal -- subclasses should connect to these
        self.internal_start_transition = self.add_basic_transition('internal-start')

        self.internal_failure_place = self.add_place('internal-failure')
        self.internal_success_place = self.add_place('internal-success')

        # Transitions to observe -- owners and subclasses may observe these
        self.start_transition = self.add_basic_transition(name='start')
        self.bridge_transitions(self.start_transition, self.internal_start_transition)

        self.failure_transition = self.add_basic_transition(name='failure')
        self.failure_transition.add_arc_in(self.internal_failure_place)

        self.success_transition = self.add_basic_transition(name='success')
        self.failure_transition.add_arc_in(self.internal_success_place)


class GenomeNetBase(SimplifiedSuccessFailureNet):
    def __init__(self, name, operation_id, parent_operation_id=None):
        SimplifiedSuccessFailureNet.__init__(self, name=name)
        self.operation_id = operation_id
        self.parent_operation_id = parent_operation_id
