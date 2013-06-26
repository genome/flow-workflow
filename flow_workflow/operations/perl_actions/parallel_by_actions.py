from flow.petri_net.actions.base import BasicActionBase, BarrierActionBase
from flow_workflow.io import load_input, store_output
from twisted.internet import defer


class ParallelBySplit(BasicActionBase):
    requrired_arguments = ['parallel_property', 'input_connections',
            'operation_id']

    def execute(self, net, color_descriptor, active_tokens, service_interfaces):
        operation_id = self.args['operation_id']
        input_connections = self.args['input_connections']
        parallel_property = self.args['parallel_property']
        parallel_input = load_input(net=net,
                input_connections=input_connections,
                property=parallel_property)

        # store the elements of parallel_input separately
        # so that they can be loaded by the target_net's action(s)
        source_operation_id, source_property = self._determine_input_source(
                input_connections=input_connections,
                name=parallel_property)
        for parallel_idx, value in enumerate(parallel_input):
            store_output(net=net,
                    operation_id=source_operation_id,
                    name=source_property,
                    value=value,
                    parallel_idx=parallel_idx)

        # create tokens to return
        parallel_size = len(parallel_input)
        workflow_data = io.extract_workflow_data(active_tokens)
        tokens = self._create_tokens(num_tokens=parallel_size,
                color_descriptor=color_descriptor,
                workflow_data=workflow_data, net=net)

        return tokens, defer.succeed(None)

    @staticmethod
    def _determine_input_source(input_connections, name):
        for src_id, prop_hash in input_connections.iteritems():
            for dst_prop, src_prop in prop_hash.iteritems():
                if name == dst_prop:
                    return src_id, src_prop
        raise KeyError("Input %s not found in input_connections (%s)" %
                (name, input_connections))


    def _create_tokens(num_tokens, color_descriptor, workflow_data, net):
        new_color_group = net.add_color_group(size=num_tokens,
                parent_color=color_descriptor.color,
                parent_color_group=color_descriptor.group.idx)


        tokens = []
        for parallel_idx in xrange(num_tokens):
            color = new_color_group.begin + parallel_idx

            data = {'workflow_data': copy.copy(workflow_data)}
            data['workflow_data']['parallel_idx'] = parallel_idx
            data['workflow_data']['parallel_size'] = num_tokens

            tokens.append(net.create_token(color=color,
                color_group_idx=new_color_group.idx, data=data))

        return tokens


class ParallelByJoin(BarrierActionBase):
    requrired_arguments = ['output_properties', 'operation_id']
    def execute(self, net, color_descriptor, active_tokens, service_interfaces):
        workflow_data = io.extract_workflow_data(active_tokens)
        parallel_size = workflow_data['parallel_size']
        del workflow_data['parallel_size']
        del workflow_data['parallel_idx']

        # find all outputs, form them into arrays
        # and then save them
        output_properties = self.args['output_properties']
        operation_id = self.args['operation_id']
        for prop in output_properties:
            array_value = []
            for parallel_idx in xrange(parallel_size):
                array_value.append(io.load_output(
                        net=net,
                        operation_id=operation_id,
                        property=prop,
                        parallel_idx=parallel_idx))
            io.store_output(net=net,
                    operation_id=operation_id,
                    property=prop,
                    value=array_value)

        data = {'workflow_data': workflow_data}
        token = net.create_token(color=color_descriptor.group.parent_color,
            color_group_idx=color_descriptor.group.parent_color_group_idx,
            data=data)

        return [token], defer.succeed(None)

class ParallelByFail(BasicActionBase):
    def execute(self, net, color_descriptor, active_tokens, service_interfaces):
        # created token should have workflow_data on it so observers can
        # know what parallel_idx failed.
        workflow_data = io.extract_workflow_data(active_tokens)
        data = {'workflow_data': workflow_data}
        token = net.create_token(color=color_descriptor.group.parent_color,
            color_group_idx=color_descriptor.group.parent_color_group_idx,
            data=data)

        return [token], defer.succeed(None)
