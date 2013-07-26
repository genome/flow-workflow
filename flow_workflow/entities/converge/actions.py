from flow_workflow import factory
from flow.petri_net.actions.base import BasicActionBase
from flow_workflow import io
from flow_workflow.parallel_id import ParallelIdentifier
from twisted.internet import defer


# This function mimics legacy Workflow's strange "Converge" operation.
# It 'converges' all outputs into one with the name of the first output_property
def order_outputs(inputs, input_property_order, output_properties):
    out_list = [inputs[x] for x in input_property_order]
    return {
        output_properties[0]: out_list,
        'result': "BQcKATE=\n", # encoded value of 1
    }


class ConvergeAction(BasicActionBase):
    required_arguments = ["operation_id", "input_property_order",
            "output_properties"]

    def execute(self, net, color_descriptor, active_tokens, service_interfaces):
        workflow_data = io.extract_workflow_data(net, active_tokens)
        parallel_id = ParallelIdentifier(workflow_data.get('parallel_id', []))

        outputs = self.converge_inputs(net=net, parallel_id=parallel_id)
        io.store_outputs(net=net, operation_id=self.args['operation_id'],
                outputs=outputs, parallel_id=list(parallel_id))

        output_token = net.create_token(color=color_descriptor.color,
                color_group_idx=color_descriptor.group.idx,
                data={'workflow_data': workflow_data})

        return [output_token], defer.succeed(None)

    def converge_inputs(self, net, parallel_id):
        operation = factory.load_operation(net, self.args['operation_id'])
        inputs = operation.load_inputs(parallel_id=parallel_id)

        return order_outputs(inputs,
                input_property_order=self.args['input_property_order'],
                output_properties=self.args['output_properties'])
