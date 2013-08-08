from flow_workflow import factory
from flow.shell_command.petri_net import actions
from flow_workflow.parallel_id import ParallelIdentifier
from flow_workflow.historian.operation_data import OperationData
from twisted.python.procutils import which

import logging


LOG = logging.getLogger(__name__)


FLOW_PATH = which('flow')[0]


def _parallel_id_from_workflow_data(workflow_data):
    return ParallelIdentifier(workflow_data.get('parallel_id', []))


class PerlAction(object):
    required_arguments = ['operation_id', 'method', 'action_type', 'action_id']


    def environment(self, net, color_descriptor):
        env = net.constant('environment', {})

        operation_id = self.args['operation_id']

        operation = factory.load_operation(net, operation_id)
        operation_data = OperationData(net_key=net.key,
                operation_id=operation_id,
                color=color_descriptor.color)

        env['FLOW_WORKFLOW_OPERATION_DATA'] = operation_data.dumps()
        env['FLOW_PARENT_WORKFLOW_LOG_DIR'] = operation.log_dir
        return env

    def command_line(self, net, token_data):
        cmd_line = [FLOW_PATH, 'workflow-wrapper',
                '--method', self.args['method'],
                '--action-type', self.args['action_type'],
                '--action-id', self.args['action_id'],
                '--net-key', net.key,
                '--operation-id', self.args['operation_id'],
        ]

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
