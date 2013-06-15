from flow.petri_net import future
from flow.shell_command.petri_net import future_nets
from flow_workflow.petri_net.actions import perl_action
from flow_workflow.petri_net.future_nets.base import GenomeNetBase

import abc
import copy


class GenomePerlActionStepNetBase(future_nets.ShellCommandNet):
    def __init__(self, name='', **action_args):
        future_nets.ShellCommandNet.__init__(self,
                name=name, **action_args)

        # XXX Attach historian transition observers


class GenomeExecuteNet(GenomePerlActionStepNetBase):
    def __init__(self, name='', remote_execute=True, **action_args):
        if remote_execute:
            self.DISPATCH_ACTION = perl_action.GenomeLSFExecuteAction
        else:
            self.DISPATCH_ACTION = perl_action.GenomeForkExecuteAction

        GenomePerlActionStepNetBase.__init__(self, **action_args)

class GenomeShortcutNet(GenomePerlActionStepNetBase):
    DISPATCH_ACTION = perl_action.GenomeShortcutAction


class GenomePerlActionNet(GenomeNetBase):
    __metaclass__ = abc.ABCMeta

    @abc.abstractproperty
    def action_type(self):
        pass

    def __init__(self, name, operation_id, input_connections,
            stderr, stdout, resources,
            remote_execute=True, project_name=''):
        GenomeNetBase.__init__(self, name=name, operation_id=operation_id)

        base_action_args = {
            'operation_id': operation_id,
            'input_connections': input_connections,
            'stderr': stderr,
            'stdout': stdout,
            'resources': resources,
        }
        lsf_options = {'project': project_name}

        shortcut_net = self.add_subnet(GenomeShortcutNet,
                **base_action_args)

        execute_action_args = copy.copy(base_action_args)
        execute_action_args['lsf_options'] = lsf_options
        execute_net = self.add_subnet(GenomeExecuteNet,
                remote_execute=remote_execute,
                **execute_action_args)

        # Connect subnets
        self.start_transition = self.bridge_places(
                self.internal_start_place, shortcut_net.start_place,
                name='start')
        self.bridge_places(shortcut_net.success_place,
                self.internal_success_place)

        self.shortcut_failure_transition = self.bridge_places(
                shortcut_net.failure_place, execute_net.start_place,
                name='shortcut-failed')
        self.bridge_places(execute_net.failure_place,
                self.internal_failure_place)
        self.bridge_places(execute_net.success_place,
                self.internal_success_place)

        # XXX Attach historian observers


class GenomeCommandNet(GenomePerlActionNet):
    action_type = 'command'


class GenomeEventNet(GenomePerlActionNet):
    action_type = 'event'
