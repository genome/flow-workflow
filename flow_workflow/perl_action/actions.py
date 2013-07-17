from flow.petri_net.actions.base import BasicActionBase
from flow.shell_command.petri_net import actions
from flow_workflow.parallel_id import ParallelIdentifier
from twisted.python.procutils import which

import json
import logging


LOG = logging.getLogger(__name__)


FLOW_PATH = which('flow')[0]


def _parallel_id_from_workflow_data(workflow_data):
    return ParallelIdentifier(workflow_data.get('parallel_id', []))


class PerlAction(object):
    required_arguments = ['operation_id', 'input_connections', 'method',
            'action_type', 'action_id']


    def environment(self, net):
        env = net.constant('environment', {})
        parent_id = '%s %s' % (net.key, self.args['operation_id'])

        env['FLOW_WORKFLOW_PARENT_ID'] = parent_id

        return env

    def command_line(self, net, token_data):
        cmd_line = [FLOW_PATH, 'workflow-wrapper',
                '--method', self.args['method'],
                '--action-type', self.args['action_type'],
                '--action-id', self.args['action_id'],
                '--net-key', net.key,
                '--operation-id', self.args['operation_id'],
                '--input-connections',
                        json.dumps(self.args['input_connections'])]

        parallel_id = self.get_parallel_id(token_data)
        if len(parallel_id):
            cmd_line.extend(['--parallel-id', parallel_id.serialize()])

        return map(str, cmd_line)

    def get_parallel_id(self, token_data):
        return _parallel_id_from_workflow_data(
                token_data.get('workflow_data', {}))


class ForkAction(PerlAction, actions.ForkDispatchAction):
    pass

class LSFAction(PerlAction, actions.LSFDispatchAction):
    pass
