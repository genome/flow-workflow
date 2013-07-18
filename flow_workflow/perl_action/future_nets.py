from flow.shell_command.petri_net.future_nets import ShellCommandNet
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
