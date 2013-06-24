from flow.petri_net.actions.base import BasicActionBase
from flow_workflow.io import load
from flow_workflow.io import store
from twisted.internet import defer

import logging


LOG = logging.getLogger(__name__)

class StoreInputsAsOutputsAction(BasicActionBase):
    required_arguments = ["operation_id", "input_connections"]

    def execute(self, net, color_descriptor, active_tokens, service_interfaces):
        operation_id = self.args["operation_id"]
        input_connections = self.args["input_connections"]

        workflow_data = load.extract_data_from_tokens(active_tokens)
        parallel_idx = workflow_data['parallel_idx']

        inputs = io.load_input(net=net, input_connections=input_connections,
                parallel_idx=parallel_idx)
        io.store_outputs(net=net, operation_id=operation_id,
                outputs=inputs, parallel_idx=parallel_idx)

        data = {'workflow_data':workflow_data}
        output_token = net.create_token(color=color_descriptor.color,
                color_group_idx=color_descriptor.group.idx,
                data=workflow_data)

        return [output_token], defer.succeed(None)
