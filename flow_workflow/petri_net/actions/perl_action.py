from flow.shell_command.petri_net import actions

import logging
import sys


LOG = logging.getLogger(__name__)


class GenomePerlAction(object):
    required_arguments = ['operation_id', 'action_type', 'action_id']

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
        parallel_idx = workflow_data.get('parallel_idx', 0)

        return map(str, ['flow', 'workflow-wrapper',
                '--action-type', self.args["action_type"],
                '--method', self.method,
                '--action-id', self.args["action_id"],
                '--operation-id', self.args['operation_id'],
                '--parallel-idx', parallel_idx ])


class GenomeShortcutAction(GenomePerlAction, actions.ForkDispatchAction):
    method = 'shortcut'


class GenomeExecuteAction(GenomePerlAction):
    method = 'execute'

class GenomeForkExecuteAction(GenomeExecuteAction, actions.ForkDispatchAction):
    pass

class GenomeLSFExecuteAction(GenomeExecuteAction, actions.LSFDispatchAction):
    pass

