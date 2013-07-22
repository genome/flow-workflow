from flow.petri_net.future import FutureAction
from flow_workflow.parallel_by import actions
from flow_workflow.future_nets import WorkflowNetBase
from flow_workflow.historian.new_action import UpdateOperationStatus


class ParallelByNet(WorkflowNetBase):
    """
    Make a given <target_net> run parallel on the given <parallel_property>.
    The target_net must be a WorkflowNetBase net.
    """
    def __init__(self, target_net, parallel_property):

        self.target_net = target_net

        operation_id = target_net.operation_id
        name = target_net.name
        WorkflowNetBase.__init__(self, operation_id=operation_id, name=name)


        self.subnets.add(target_net)

        # split_transition
        split_action = FutureAction(cls=actions.ParallelBySplit,
                operation_id=operation_id,
                parallel_property=parallel_property)
        self.split_transition = self.add_basic_transition(
                name='ParallelBy(%s) split' % operation_id,
                action=split_action)
        self.starting_split_place = self.bridge_transitions(
                self.internal_start_transition,
                self.split_transition,
                name='starting-split')
        self.succeeding_split_place = self.bridge_transitions(
                self.split_transition,
                target_net.start_transition,
                name='succeeding-split')

        self.observe_transition(self.split_transition,
                FutureAction(UpdateOperationStatus,
                    operation_id=self.operation_id, status='new'))

        # join_transition
        join_action = FutureAction(cls=actions.ParallelByJoin,
                operation_id=operation_id)
        self.join_transition = self.add_barrier_transition(
                name='ParallelBy(%s) join' % operation_id,
                action=join_action)
        self.starting_join_place = self.bridge_transitions(
                target_net.success_transition,
                self.join_transition,
                name='starting-join')
        self.succeeding_join_place = self.bridge_transitions(
                self.join_transition,
                self.internal_success_transition,
                name='succeeding-join')

        # fail transition
        target_fail_action = FutureAction(cls=actions.ParallelByFail)
        self.target_fail_transition = self.add_basic_transition(
                name='ParallelBy(%s) fail' % operation_id,
                action=target_fail_action)
        self.failing_target_place = self.bridge_transitions(
                target_net.failure_transition,
                self.target_fail_transition,
                name='failing-target')
        self.failing_place = self.bridge_transitions(
                self.target_fail_transition,
                self.internal_failure_transition,
                name='failing')
