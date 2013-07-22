from flow.petri_net.future import FutureAction
from flow.petri_net.future import FutureAction
from flow_workflow.future_nets import WorkflowNetBase
from flow_workflow.historian import new_action


class ModelNet(WorkflowNetBase):
    def __init__(self, name, operation_id, subnets, edges):
        WorkflowNetBase.__init__(self, operation_id=operation_id, name=name)

        for name, subnet in subnets.iteritems():
            self.subnets.add(subnet)

        for source_name, dest_names in edges.iteritems():
            for dest_name in dest_names:
                self.bridge_transitions(subnets[source_name].success_transition,
                        subnets[dest_name].start_transition)

        self.starting_place = self.bridge_transitions(
                self.internal_start_transition,
                subnets['input connector'].start_transition,
                name='starting')

        self.observe_transition(self.internal_start_transition,
                FutureAction(new_action.UpdateChildrenStatuses,
                    operation_id=operation_id, status='new'))

        self.succeeding_place = self.bridge_transitions(
                subnets['output connector'].success_transition,
                self.internal_success_transition,
                name='succeeding')

        self.failing_place = self.join_transitions_as_or(
                self.internal_failure_transition,
                [s.failure_transition for s in subnets.itervalues()],
                name='failing')


        self.observe_transition(self.internal_start_transition,
                FutureAction(new_action.UpdateOperationStatus,
                    operation_id=operation_id, status='running',
                    calculate_start_time=True))

        self.observe_transition(self.internal_success_transition,
                FutureAction(new_action.UpdateOperationStatus,
                    operation_id=operation_id, status='done',
                    calculate_end_time=True))

        self.observe_transition(self.internal_failure_transition,
                FutureAction(new_action.UpdateOperationStatus,
                    operation_id=operation_id, status='crashed',
                    calculate_end_time=True))
