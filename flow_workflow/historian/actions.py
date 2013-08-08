from abc import abstractmethod
from flow.petri_net.actions.base import BasicActionBase
from flow.util.containers import head
from flow_workflow import factory
from flow_workflow.historian.operation_data import OperationData
from flow_workflow.parallel_id import ParallelIdentifier
from time import localtime, strftime
from twisted.internet import defer

import logging


LOG = logging.getLogger(__name__)


class HistorianActionBase(BasicActionBase):
    required_args = ['operation_id']

    def execute(self, net, color_descriptor, active_tokens, service_interfaces):
        if env_is_perl_true(net, 'UR_DBI_NO_COMMIT'):
            LOG.debug('UR_DBI_NO_COMMIT is set, not updating status.')
            return [], defer.succeed(None)

        historian = service_interfaces['workflow_historian']

        token_data = net.token(head(active_tokens)).data.value
        workflow_data = token_data.get('workflow_data', {})
        parallel_id = ParallelIdentifier(workflow_data.get('parallel_id', []))

        deferred = self._execute(historian=historian, net=net,
                color_descriptor=color_descriptor, parallel_id=parallel_id,
                token_data=token_data)

        return [], deferred


    @abstractmethod
    def _execute(self, historian, net, color_descriptor, parallel_id,
            token_data):
        raise NotImplementedError()

    def operation(self, net):
        return factory.load_operation(net, self.args['operation_id'])


    def update_operation_status(self, historian, net, operation,
            color_descriptor, parallel_id, status, token_data,
            **additional_properties):
        operation_data = OperationData(net_key=operation.net_key,
                operation_id=operation.operation_id,
                color=color_descriptor.color)
        fields = {
                'operation_data': operation_data.to_dict,
                'name': operation.name,
                'workflow_plan_id': net.constant('workflow_plan_id'),
                'user_name': net.constant('user_name'),
                'status': status,
                }

        fields.update(additional_properties)
        fields.update(get_parent_fields(operation, parallel_id,
            color_descriptor))
        fields.update(get_peer_fields(operation, parallel_id, color_descriptor))

        fields.update(self.get_shell_command_fields(token_data))
        fields.update(self.get_log_fields(operation.log_manager, parallel_id))

        return historian.update(**fields)

    def get_shell_command_fields(self, token_data):
        fields = {}
        if 'job_id' in token_data:
            fields['dispatch_id'] = '%s%s' % (
                    self.args.get('preprend_job_id_with', ''),
                    token_data['job_id'])

        if self.args.get('calculate_start_time', False):
            fields['start_time'] = self.timestamp

        if self.args.get('calculate_end_time', False):
            fields['end_time'] = self.timestamp

        if 'exit_code' in token_data:
            fields['exit_code'] = token_data['exit_code']

        return fields

    def get_log_fields(self, log_manager, parallel_id):
        fields = {}
        if log_manager.stderr_log_path(parallel_id):
            fields['stderr'] = log_manager.stderr_log_path(parallel_id)
        if log_manager.stdout_log_path(parallel_id):
            fields['stdout'] = log_manager.stdout_log_path(parallel_id)

        return fields

    @property
    def timestamp(self):
        now = self.connection.time()
        # convert (sec, microsec) from redis to floating point sec
        now = now[0] + now[1] * 1e-6

        return strftime("%Y-%m-%d %H:%M:%S", localtime(now)).upper()


def get_parent_fields(operation, parallel_id, color_descriptor):
    if operation.parent.operation_id:
        result = {}
        if operation.parent_is_foreign:
            result['is_subflow'] = True

        if parallel_id.refers_to(operation):
            color = color_descriptor.group.parent_color
        else:
            color = color_descriptor.color
        operation_data = OperationData(net_key=operation.parent.net_key,
                operation_id=operation.parent.operation_id,
                color=color)
        result['parent_operation_data'] = operation_data.to_dict
        return result
    else:
        return {}


def get_peer_fields(operation, parallel_id, color_descriptor):
    if parallel_id.refers_to(operation):
        operation_data = OperationData(net_key=operation.net_key,
                operation_id=operation.operation_id,
                color=color_descriptor.group.begin)
        return {'peer_operation_data': operation_data.to_dict,
                'parallel_index':parallel_id.index}
    else:
        return {}


class UpdateChildrenStatuses(HistorianActionBase):
    required_args = ['operation_id', 'status']

    def _execute(self, historian, net, color_descriptor, parallel_id,
            token_data):
        deferreds = []
        operation = self.operation(net)
        for child_operation in operation.iter_children():
            deferred = self.update_operation_status(historian, net,
                    child_operation, color_descriptor, parallel_id,
                    token_data=token_data, status=self.args['status'])
            deferreds.append(deferred)

        return defer.gatherResults(deferreds)


class UpdateOperationStatus(HistorianActionBase):
    required_args = ['operation_id', 'status']

    def _execute(self, historian, net, color_descriptor, parallel_id,
            token_data):
        operation = self.operation(net)

        return self.update_operation_status(historian, net, operation,
                color_descriptor, parallel_id, token_data=token_data,
                status=self.args['status'])


class DeletePlaceholderOperation(HistorianActionBase):
    required_args = ['operation_id']

    def _execute(self, historian, net, color_descriptor, parallel_id,
            token_data):
        return historian.delete(OperationData(
                operation_id=self.args['operation_id'],
                color=color_descriptor.color, net_key=net.key),
            workflow_plan_id=net.constant('workflow_plan_id'))


def env_is_perl_true(net, varname):
    env = net.constant('environment', {})
    var = env.get(varname)
    return var_is_perl_true(var)


_PERL_FALSE_VALUES = set([
    '0',
    '',
])
def var_is_perl_true(var):
    return var and (str(var) not in _PERL_FALSE_VALUES)
