from flow.petri_net.future import FutureAction
from flow_workflow.petri_net.actions.parallel_by as pb
from flow_workflow.petri_net.future_nets.base import GenomeNetBase


class GenomeParallelByNet(GenomeNetBase):
    """
    Make a given <target_net> run parallel on the given <target_input_name>.
    The target_net must be a success-failure net.
    """
    def __init__(self, name, operation_id, target_net, parallel_property,
            input_connections, output_properties, parent_operation_id=None):
        GenomeNetBase.__init__(self, name=name, operation_id=operation_id,
                parent_operation_id=parent_operation_id)

        # split_transition
        split_action = FutureAction(cls=pb.ParallelBySplit,
                operation_id=self.operation_id,
                parallel_property=parallel_property,
                input_connections=input_connections)
        self.split_transition = self.add_basic_transition(
                name='ParallelBy(%s) split' % operation_id,
                action=split_action)
        self.split_transition.add_arc_in(self.internal_start_place)
        self.split_transition.add_arc_out(target_net.start_place)

        # join_transition
        join_action = FutureAction(cls=pb.ParallelByJoin,
                operation_id=operation_id,
                output_properties=output_properties)
        self.join_transition = self.add_barrier_transition(
                name='ParallelBy(%s) join' % operation_id,
                action=join_action)
        self.join_transition.add_arc_in(target_net.success_place)
        self.join_transition.add_arc_out(self.internal_success_place)

        # fail transition
        fail_action = FutureAction(cls=pb.ParallelByFail)
        self.fail_transition = self.add_basic_transition(
                name='ParallelBy(%s) fail' % operation_id,
                action=fail_action)
        self.fail_transition.add_arc_in(target_net.failure_place)
        self.fail_transition.add_arc_out(self.internal_failure_place)
