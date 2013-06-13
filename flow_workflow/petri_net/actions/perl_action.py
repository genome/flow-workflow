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
        return self._update_environment(net, env)

    def command_line(self, net, token_data):
        parallel_idx = token_data.get('parallel_idx', 0)

        return map(str, ['flow', 'workflow-wrapper',
                '--action-type', self.args["action_type"],
                '--method', self.method,
                '--action-id', self.args["action_id"],
                '--operation-id', self.args['operation_id'],
                '--parallel-idx', parallel_idx ])


    def _update_environment(self, net, env):
        parent_id = '%s %s' % (net.key, self.args['operation_id'])
        env['FLOW_WORKFLOW_PARENT_ID'] = parent_id

        LOG.debug('Setting environment variable FLOW_WORKFLOW_PARENT_ID=%s',
                parent_id)

        return env


class GenomeShortcutAction(GenomePerlAction, actions.ForkDispatchAction):
    method = 'shortcut'


class GenomeExecuteAction(GenomePerlAction):
    method = 'execute'

class GenomeForkExecuteAction(GenomeExecuteAction, actions.ForkDispatchAction):
    pass

class GenomeLSFExecuteAction(GenomeExecuteAction, actions.LSFDispatchAction):
    pass


# XXX This can just be put in lsf_options on net creation
#    def _executor_options(self, input_data_key, net):
#        executor_options = actions.LSFDispatchAction._executor_options(
#                self, input_data_key, net)
#
#        lsf_project = net.constant('lsf_project')
#        if lsf_project:
#            executor_options['project'] = lsf_project
#
#        return executor_options
