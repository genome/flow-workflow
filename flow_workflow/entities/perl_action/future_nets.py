from flow.petri_net.future import FutureAction
from flow.shell_command.petri_net.future_nets import ShellCommandNet
from flow_workflow.entities.perl_action import actions
from flow_workflow.future_nets import WorkflowNetBase


class PerlActionNet(WorkflowNetBase):
    """
    A success-failure net that internally tries to shortcut and then to
    execute a perl action.
    """

    def __init__(self, name, operation_id, input_connections,
            resources, stderr, stdout,
            action_type, action_id,
            shortcut_action_class, execute_action_class,
            project_name='', parent_operation_id=None):
        WorkflowNetBase.__init__(self, name=name,
                operation_id=operation_id,
                input_connections=input_connections,
                resources=resources,
                parent_operation_id=parent_operation_id)

        base_action_args = {
            'operation_id': operation_id,
            'input_connections': input_connections,
            'action_id': action_id,
            'action_type': action_type,
            'stderr': stderr,
            'stdout': stdout,
            'resources': resources,
        }
        self.shortcut_net = self.add_subnet(ShellCommandNet, name=name,
                dispatch_action_class=shortcut_action_class,
                method='shortcut', **base_action_args)

        lsf_options = {'project': project_name}
        self.execute_net = self.add_subnet(ShellCommandNet, name=name,
                dispatch_action_class=execute_action_class,
                lsf_options=lsf_options, method='execute',
                **base_action_args)

        # Connect subnets
        self.starting_shortcut_place = self.bridge_transitions(
                self.internal_start_transition,
                self.shortcut_net.start_transition,
                name='starting-shortcut')
        self.starting_execute_place = self.bridge_transitions(
                self.shortcut_net.failure_transition,
                self.execute_net.start_transition,
                name='starting-execute')

        self.succeeding_place = self.join_transitions_as_or(
                self.internal_success_transition,
                [self.shortcut_net.success_transition,
                    self.execute_net.success_transition],
                name='succeeding')
        self.failing_place = self.bridge_transitions(
                self.execute_net.failure_transition,
                self.internal_failure_transition,
                name='failing')
        # XXX Attach historian observers


class ParallelByNet(WorkflowNetBase):
    """
    Make a given <target_net> run parallel on the given <parallel_property>.
    The target_net must be a WorkflowNetBase net.
    """
    def __init__(self, target_net, parallel_property, output_properties):
        # adopt values from the target_net
        self.target_net = target_net
        operation_id = target_net.operation_id
        parent_operation_id = target_net.parent_operation_id
        name = "%s (parallel_by harness)" % target_net.name
        input_connections = target_net.input_connections
        resources = target_net.resources

        WorkflowNetBase.__init__(self, name=name, operation_id=operation_id,
                input_connections=input_connections,
                resources=resources,
                parent_operation_id=parent_operation_id)

        # add target_net to self
        self.subnets.add(target_net)

        # split_transition
        split_action = FutureAction(cls=actions.ParallelBySplit,
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
        join_action = FutureAction(cls=actions.ParallelByJoin,
                operation_id=operation_id, output_properties=output_properties)
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
