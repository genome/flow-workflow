from flow.petri_net.future import FutureAction
from flow_workflow.operations.perl_actions import parallel_by_actions as pb
from flow_workflow.operations.workflow_net_base import WorkflowNetBase


class ParallelByNet(WorkflowNetBase):
    """
    Make a given <target_net> run parallel on the given <parallel_property>.
    The target_net must be a WorkflowNetBase net.
    """
    def __init__(self, target_net, parallel_property):
        # adopt values from the target_net
        self.target_net = target_net
        operation_id = target_net.operation_id
        parent_operation_id = target_net.parent_operation_id
        name = "%s (parallel_by harness)" % target_net.name
        input_connections = target_net.input_connections
        output_properties = target_net.output_properties
        resources = target_net.resources

        WorkflowNetBase.__init__(self, name=name, operation_id=operation_id,
                input_connections=input_connections,
                output_properties=output_properties,
                resources=resources,
                parent_operation_id=parent_operation_id)

        # add target_net to self
        self.subnets.add(target_net)

        # split_transition
        split_action = FutureAction(cls=pb.ParallelBySplit,
                operation_id=self.operation_id,
                parallel_property=parallel_property,
                input_connections=input_connections)
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

        # join_transition
        join_action = FutureAction(cls=pb.ParallelByJoin,
                operation_id=operation_id,
                output_properties=output_properties)
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
        target_fail_action = FutureAction(cls=pb.ParallelByFail)
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
