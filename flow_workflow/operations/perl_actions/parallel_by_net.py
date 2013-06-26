from flow.petri_net.future import FutureAction
from flow_workflow.petri_net.actions.parallel_by as pb
from flow_workflow.petri_net.future_nets.base import GenomeNetBase


class GenomeParallelByNet(GenomeNetBase):
    """
    Make a given <target_net> run parallel on the given <parallel_property>.
    The target_net must be a GenomeNetBase net.
    """
    def __init__(self, target_net, parallel_property):
        # adopt values from the target_net
        operation_id = target_net.operation_id
        parent_operation_id = target_net.parent_operation_id
        name = "%s (parallel_by harness)" % target_net.name
        input_connections = target_net.input_connections
        output_properties = target_net.output_properties

        GenomeNetBase.__init__(self, name=name, operation_id=operation_id,
                parent_operation_id=parent_operation_id)

        # add target_net to self
        self.add_subnet(target_net)

        # split_transition
        split_action = FutureAction(cls=pb.ParallelBySplit,
                operation_id=self.operation_id,
                parallel_property=parallel_property,
                input_connections=input_connections)
        self.split_transition = self.add_basic_transition(
                name='ParallelBy(%s) split' % operation_id,
                action=split_action)
        self.bridge_transitions(self.internal_start_transition,
                self.split_transition)
        self.bridge_transitions(self.split_transition,
                target_net.start_transition)

        # join_transition
        join_action = FutureAction(cls=pb.ParallelByJoin,
                operation_id=operation_id,
                output_properties=output_properties)
        self.join_transition = self.add_barrier_transition(
                name='ParallelBy(%s) join' % operation_id,
                action=join_action)
        self.bridge_transitions(target_net.success_transition,
                self.join_transition)
        self.join_transition.add_arc_out(self.success_place)

        # fail transition
        target_fail_action = FutureAction(cls=pb.ParallelByFail)
        self.target_fail_transition = self.add_basic_transition(
                name='ParallelBy(%s) fail' % operation_id,
                action=fail_action)
        self.bridge_transitions(target_net.failure_transition,
                self.target_fail_transition)
        self.fail_transition.add_arc_out(self.internal_failure_place)
