from flow.shell_command.petri_net.future_nets import ShellCommandNet
from flow_workflow.operations.perl_actions import actions
from flow_workflow.operations.workflow_net_base import WorkflowNetBase

import abc
import copy


class StepNetBase(ShellCommandNet):
    def __init__(self, name='', **action_args):
        ShellCommandNet.__init__(self,
                name=name, **action_args)
        # XXX Attach historian transition observers


class ExecuteNet(StepNetBase):
    def __init__(self, name='', remote_execute=True, **action_args):
        if remote_execute:
            self.DISPATCH_ACTION = actions.WorkflowLSFExecuteAction
        else:
            self.DISPATCH_ACTION = actions.WorkflowForkExecuteAction

        StepNetBase.__init__(self, **action_args)

class ShortcutNet(StepNetBase):
    DISPATCH_ACTION = actions.ShortcutAction


class PerlActionNet(WorkflowNetBase):
    """
    A success-failure net that internally tries to shortcut and then to
    execute a perl action.
    """
    __metaclass__ = abc.ABCMeta

    @abc.abstractproperty
    def action_type(self):
        pass

    def __init__(self, name, operation_id, input_connections,
            output_properties, resources, stderr, stdout, action_id,
            remote_execute=True, project_name='', parent_operation_id=None):
        WorkflowNetBase.__init__(self, name=name,
                operation_id=operation_id,
                input_connections=input_connections,
                output_properties=output_properties,
                resources=resources,
                parent_operation_id=parent_operation_id)

        base_action_args = {
            'operation_id': operation_id,
            'input_connections': input_connections,
            'action_id': action_id,
            'action_type':self.action_type,
            'stderr': stderr,
            'stdout': stdout,
            'resources': resources,
        }
        self.shortcut_net = self.add_subnet(ShortcutNet,
                **base_action_args)

        lsf_options = {'project': project_name}
        execute_action_args = copy.copy(base_action_args)
        execute_action_args['lsf_options'] = lsf_options
        self.execute_net = self.add_subnet(ExecuteNet,
                remote_execute=remote_execute,
                **execute_action_args)

        # Connect subnets
        self.starting_shortcut_place = self.bridge_transitions(
                self.internal_start_transition,
                self.shortcut_net.start_transition,
                name='starting-shortcut')
        self.starting_execute_place = self.bridge_transitions(
                self.shortcut_net.failure_transition,
                self.execute_net.start_transition,
                name='starting-execute')

        self.succeeding_place = self.bridge_transitions(
                self.shortcut_net.success_transition,
                self.execute_net.internal_success_transition,
                name='succeeding')
        self.succeeding_place.add_arc_in(
                self.execute_net.success_transition)
        self.failing_place = self.bridge_transitions(
                self.execute_net.failure_transition,
                self.internal_failure_transition,
                name='failing')
        # XXX Attach historian observers


class CommandNet(PerlActionNet):
    action_type = 'command'


class EventNet(PerlActionNet):
    action_type = 'event'
