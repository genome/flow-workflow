from flow.shell_command.petri_net import actions

import logging
import sys


LOG = logging.getLogger(__name__)


class GenomePerlAction(object):
    required_arguments = ['operation_id', 'input_connections', 
            'action_type', 'action_id']

    @property
    def method(self):
        raise NotImplementedError('%s must provide a method property'
                % self.__class__)


    def environment(self, net):
        env = net.constant('environment', {})
        parent_id = '%s %s' % (net.key, self.args['operation_id'])

        LOG.debug('Setting environment variable FLOW_WORKFLOW_PARENT_ID=%s',
                parent_id)
        env['FLOW_WORKFLOW_PARENT_ID'] = parent_id

        return env

    def command_line(self, net, workflow_data):
        cmd_line = map(str, ['flow', 'workflow-wrapper',
                '--method', self.method,
                '--action-type', self.args["action_type"],
                '--action-id', self.args["action_id"],
                '--operation-id', self.args['operation_id'],
                '--input-connections', self.args['input_connections']])
        parallel_idx = workflow_data.get('parallel_idx', None)
        if parallel_idx is not None:
            cmd_line.extend(['--parallel-idx', parallel_idx])
        return cmd_line


class GenomeShortcutAction(GenomePerlAction, actions.ForkDispatchAction):
    method = 'shortcut'


class GenomeExecuteAction(GenomePerlAction):
    method = 'execute'

class GenomeForkExecuteAction(GenomeExecuteAction, actions.ForkDispatchAction):
    pass

class GenomeLSFExecuteAction(GenomeExecuteAction, actions.LSFDispatchAction):
    pass

