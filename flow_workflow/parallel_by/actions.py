from flow.petri_net.actions.base import BasicActionBase, BarrierActionBase
from flow_workflow import io
from flow_workflow.parallel_id import ParallelIdentifier
from twisted.internet import defer
from flow_workflow import factory

import copy
import logging


LOG = logging.getLogger(__name__)


def _parallel_id_from_workflow_data(workflow_data):
    return ParallelIdentifier(workflow_data.get('parallel_id', []))


class ParallelBySplit(BasicActionBase):
    requrired_arguments = ['parallel_property', 'input_connections',
            'operation_id']

    def execute(self, net, color_descriptor, active_tokens, service_interfaces):
        workflow_data = io.extract_workflow_data(net, active_tokens)

        parallel_property = self.args['parallel_property']
        parallel_id = _parallel_id_from_workflow_data(workflow_data)

        operation = factory.load_operation(net=net,
                operation_id=self.args['operation_id'])

        parallel_input = operation.load_input(
                name=parallel_property,
                parallel_id=parallel_id)

        self.store_parallel_input(operation=operation,
                parallel_input=parallel_input,
                parallel_property=parallel_property,
                parallel_id=parallel_id)

        tokens = self._create_tokens(num_tokens=len(parallel_input),
                color_descriptor=color_descriptor,
                workflow_data=workflow_data, net=net)

        return tokens, defer.succeed(None)

    def store_parallel_input(self, operation, parallel_property, parallel_input,
            parallel_id):
        for parallel_idx, value in enumerate(parallel_input):
            operation.store_input(name=parallel_property,
                    value=value, parallel_id=parallel_id.child_identifier(
                        operation.operation_id, parallel_idx))

    def _create_tokens(self, num_tokens, color_descriptor, workflow_data, net):
        new_color_group = net.add_color_group(size=num_tokens,
                parent_color=color_descriptor.color,
                parent_color_group_idx=color_descriptor.group.idx)

        this_workflow_data = copy.copy(workflow_data)
        parallel_id = _parallel_id_from_workflow_data(workflow_data)

        operation_id = self.args['operation_id']

        tokens = []
        for parallel_idx in xrange(num_tokens):
            color = new_color_group.begin + parallel_idx

            this_workflow_data['parallel_id'] = list(
                    parallel_id.child_identifier(operation_id, parallel_idx))
            data = {'workflow_data': this_workflow_data}

            tokens.append(net.create_token(color=color,
                color_group_idx=new_color_group.idx, data=data))

        return tokens


class ParallelByJoin(BarrierActionBase):
    requrired_arguments = ['output_properties', 'operation_id']

    def execute(self, net, color_descriptor, active_tokens, service_interfaces):
        operation_id = self.args['operation_id']
        op = factory.load_operation(net, operation_id)

        parallel_size = color_descriptor.group.size

        workflow_data = io.extract_workflow_data(net, active_tokens)
        parallel_id = _parallel_id_from_workflow_data(workflow_data)
        parent_parallel_id = parallel_id.parent_identifier

        for property_name in self.args['output_properties']:
            array_value = self.collect_array_output(net=net,
                    operation=op,
                    parallel_size=parallel_size,
                    property_name=property_name,
                    parallel_id=parent_parallel_id)

            op.store_output(property_name, value=array_value,
                    parallel_id=parent_parallel_id)

        workflow_data['parallel_id'] = list(parent_parallel_id)
        token = net.create_token(color=color_descriptor.group.parent_color,
            color_group_idx=color_descriptor.group.parent_color_group_idx,
            data={'workflow_data': workflow_data})

        return [token], defer.succeed(None)

    def collect_array_output(self, net, property_name, parallel_size,
            operation, parallel_id):
        result = []
        for parallel_idx in xrange(parallel_size):
            result.append(operation.load_output(name=property_name,
                parallel_id=parallel_id.child_identifier(
                    operation.operation_id, parallel_idx)))

        return result


class ParallelByFail(BasicActionBase):
    def execute(self, net, color_descriptor, active_tokens, service_interfaces):
        # created token should have workflow_data on it so observers can
        # know what parallel_id failed.
        workflow_data = io.extract_workflow_data(net, active_tokens)
        data = {'workflow_data': workflow_data}
        token = net.create_token(color=color_descriptor.group.parent_color,
            color_group_idx=color_descriptor.group.parent_color_group_idx,
            data=data)

        return [token], defer.succeed(None)
