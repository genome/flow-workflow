from flow.petri_net.actions.base import BasicActionBase
from flow_workflow import io
from twisted.internet import defer


class CloneInputsAction(BasicActionBase):
    required_arguments = ["operation_id", "input_connections"]

    def execute(self, net, color_descriptor, active_tokens, service_interfaces):
        operation_id = self.args["operation_id"]
        input_connections = self.args["input_connections"]

        workflow_data = io.extract_workflow_data(net, active_tokens)
        parallel_id = workflow_data.get('parallel_id', {})

        inputs = io.load_input(net=net, input_connections=input_connections,
                parallel_id=parallel_id)
        io.store_outputs(net=net, operation_id=operation_id,
                outputs=inputs, parallel_id=parallel_id)

        data = {'workflow_data': workflow_data}
        output_token = net.create_token(color=color_descriptor.color,
                color_group_idx=color_descriptor.group.idx,
                data=data)

        return [output_token], defer.succeed(None)
