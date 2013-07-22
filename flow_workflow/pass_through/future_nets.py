from flow.petri_net.future import FutureAction
from flow_workflow.future_nets import WorkflowNetBase
from flow_workflow.historian.new_action import UpdateOperationStatus


class PassThroughNet(WorkflowNetBase):
    def __init__(self, operation_id, name):
        WorkflowNetBase.__init__(self, operation_id=operation_id, name=name)
        self.skipping_place = self.bridge_transitions(
                self.internal_start_transition,
                self.internal_success_transition,
                name='skipping place')

        self.observe_transition(self.internal_success_transition,
                FutureAction(UpdateOperationStatus, operation_id=operation_id,
                    status='done', calculate_start_time=True,
                    calculate_end_time=True))
