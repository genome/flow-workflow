from abc import abstractmethod
from flow.petri_net.actions.base import BasicActionBase
from flow_workflow import factory
from flow_workflow import io
from flow_workflow.parallel_id import ParallelIdentifier
from twisted.internet import defer
from flow_workflow.historian.operation_data import OperationData

class WorkflowUpdateActionBase(BasicActionBase):
    required_args = ['operation_id']

    def execute(self, net, color_descriptor, active_tokens, service_interfaces):
        if env_is_perl_true(net, 'UR_DBI_NO_COMMIT'):
            LOG.debug('UR_DBI_NO_COMMIT is set, not updating status.')
            return [], defer.succeed(None)

        historian = service_interfaces['workflow_historian']

        workflow_data = io.extract_workflow_data(net, active_tokens)
        parallel_id = ParallelIdentifier(workflow_data.get('parallel_id', []))

        deferred = self._execute(historian=historian, net=net,
                color_descriptor=color_descriptor, parallel_id=parallel_id,
                workflow_data=workflow_data)

        return [], deferred


    @abstractmethod
    def _execute(self, historian, net, color_descriptor, parallel_id,
            workflow_data):
        raise NotImplementedError()

    def operation(self, net):
        return factory.load_operation(net, self.args['operation_id'])


def update_operation_status(historian, net, operation, color_descriptor,
        parallel_id, workflow_data, status, **additional_properties):
    operation_data = OperationData(net_key=operation.net_key,
            operation_id=operation.operation_id,
            color=color_descriptor.color)
    fields = {
            'operation_data': operation_data.to_dict,
            'name': operation.name,
            'workflow_plan_id': net.constant('workflow_plan_id'),
            'status': status,
            }

    fields.update(additional_properties)
    fields.update(get_parent_fields(operation, parallel_id, color_descriptor))
    fields.update(get_peer_fields(operation, parallel_id, color_descriptor))

    return historian.update(**fields)


def get_parent_fields(operation, parallel_id, color_descriptor):
    if operation.parent.operation_id:
        if parallel_id.refers_to(operation):
            color = color_descriptor.group.parent_color
        else:
            color = color_descriptor.color
        operation_data = OperationData(net_key=operation.parent.net_key,
                operation_id=operation.parent.operation_id,
                color=color)
        return {'parent_operation_data': operation_data.to_dict}
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


class UpdateChildrenStatuses(WorkflowUpdateActionBase):
    required_args = ['operation_id', 'status']

    def _execute(self, historian, net, color_descriptor, parallel_id,
            workflow_data):
        deferreds = []
        operation = self.operation(net)
        for child_operation in operation.iter_children():
            deferred = update_operation_status(historian, net, child_operation,
                    color_descriptor, parallel_id, workflow_data,
                    status=self.args['status'])
            deferreds.append(deferred)

        return defer.gatherResults(deferreds)


class UpdateOperationStatus(WorkflowUpdateActionBase):
    required_args = ['operation_id', 'status']

    def _execute(self, historian, net, color_descriptor, parallel_id, workflow_data):
        operation = self.operation(net)

        return update_operation_status(historian, net, operation,
                    color_descriptor, parallel_id, workflow_data,
                    status=self.args['status'])


def env_is_perl_true(net, varname):
    env = net.constant('environment')
    try:
        var = env.get(varname)
        return var_is_perl_true(var)
    except:
        pass

    return False


_PERL_FALSE_VALUES = set([
    '0',
    '',
])
def var_is_perl_true(var):
    return var and (str(var) not in _PERL_FALSE_VALUES)
