from flow.petri_net.actions.base import BasicActionBase, BarrierActionBase
from flow.shell_command.petri_net import actions
from twisted.python.procutils import which
from flow_workflow import io
from twisted.internet import defer

import copy
import json
import logging
import sys


LOG = logging.getLogger(__name__)


FLOW_PATH = which('flow')[0]


class PerlAction(object):
    required_arguments = ['operation_id', 'input_connections', 'method',
            'action_type', 'action_id']


    def environment(self, net):
        env = net.constant('environment', {})
        parent_id = '%s %s' % (net.key, self.args['operation_id'])

        LOG.debug('Setting environment variable FLOW_WORKFLOW_PARENT_ID="%s"',
                parent_id)
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

        parallel_id = token_data.get('workflow_data', {}).get('parallel_id', [])
        if parallel_id:
            cmd_line.extend(['--parallel-id', json.dumps(parallel_id)])

        return map(str, cmd_line)


class ForkAction(PerlAction, actions.ForkDispatchAction):
    pass

class LSFAction(PerlAction, actions.LSFDispatchAction):
    pass


class ParallelBySplit(BasicActionBase):
    requrired_arguments = ['parallel_property', 'input_connections',
            'operation_id']

    def execute(self, net, color_descriptor, active_tokens, service_interfaces):
        workflow_data = io.extract_workflow_data(net, active_tokens)

        parallel_id = workflow_data.get('parallel_id', [])
        parallel_input = io.load_input(net=net,
                input_connections=self.args['input_connections'],
                property_name=self.args['parallel_property'],
                parallel_id=parallel_id)

        self.store_parallel_input(net, parallel_input, parallel_id)
        tokens = self._create_tokens(num_tokens=len(parallel_input),
                color_descriptor=color_descriptor,
                workflow_data=workflow_data, net=net)

        return tokens, defer.succeed(None)

    def store_parallel_input(self, net, parallel_input, parallel_id):
        operation_id = self.args['operation_id']
        parallel_id = list(parallel_id)

        source_operation_id, source_property = self.determine_input_source(
                name=self.args['parallel_property'])

        for parallel_idx, value in enumerate(parallel_input):
            parallel_id.append((operation_id, parallel_idx))
            io.store_output(net=net,
                    operation_id=source_operation_id,
                    property_name=source_property,
                    value=value,
                    parallel_id=parallel_id)
            parallel_id.pop()

    def determine_input_source(self, name):
        for src_id, prop_hash in self.args['input_connections'].iteritems():
            for dst_prop, src_prop in prop_hash.iteritems():
                if name == dst_prop:
                    return src_id, src_prop
        raise KeyError("Input %s not found in input_connections (%s)" %
                (name, self.args['input_connections']))


    def _create_tokens(self, num_tokens, color_descriptor, workflow_data, net):
        new_color_group = net.add_color_group(size=num_tokens,
                parent_color=color_descriptor.color,
                parent_color_group_idx=color_descriptor.group.idx)

        operation_id = self.args['operation_id']

        tokens = []
        for parallel_idx in xrange(num_tokens):
            color = new_color_group.begin + parallel_idx

            this_workflow_data = copy.copy(workflow_data)
            this_workflow_data.setdefault('parallel_id', [])
            this_workflow_data['parallel_id'].append(
                    (operation_id, parallel_idx))

            data = {'workflow_data': this_workflow_data}

            tokens.append(net.create_token(color=color,
                color_group_idx=new_color_group.idx, data=data))

        return tokens


class ParallelByJoin(BarrierActionBase):
    requrired_arguments = ['output_properties', 'operation_id']

    def execute(self, net, color_descriptor, active_tokens, service_interfaces):
        operation_id = self.args['operation_id']
        parallel_size = color_descriptor.group.size

        workflow_data = io.extract_workflow_data(net, active_tokens)
        parallel_id = workflow_data.get('parallel_id', [])
        parent_parallel_id = parallel_id[:-1]

        for property_name in self.args['output_properties']:
            array_value = self.collect_array_output(net=net,
                    operation_id=operation_id,
                    parallel_size=parallel_size,
                    property_name=property_name,
                    parallel_id=parallel_id)

            io.store_output(net=net,
                    operation_id=operation_id,
                    property_name=property_name,
                    value=array_value,
                    parallel_id=parent_parallel_id)

        token = net.create_token(color=color_descriptor.group.parent_color,
            color_group_idx=color_descriptor.group.parent_color_group_idx,
            data={'workflow_data': workflow_data})

        return [token], defer.succeed(None)

    def collect_array_output(self, net, property_name, parallel_size,
            operation_id, parallel_id):
        parallel_id = list(parallel_id)

        result = []
        for parallel_idx in xrange(parallel_size):
            parallel_id.append((operation_id, parallel_idx))
            result.append(io.load_output(
                    net=net,
                    operation_id=operation_id,
                    property_name=property_name,
                    parallel_id=parallel_id))
            parallel_id.pop()

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
