from flow.petri_net.actions.base import BasicActionBase
from flow_workflow.io import load
from flow_workflow.io import store
from twisted.internet import defer

import logging


LOG = logging.getLogger(__name__)


# This function mimics legacy Workflow's strange "Converge" operation.
# It only works for a single output property (to match the legacy code).
def order_outputs(inputs, input_property_order, output_properties):
    out_list = [inputs[x] for x in input_property_order]
    return {prop: out_list for prop in output_properties}


class GenomeConvergeAction(BasicActionBase):
    required_arguments =["operation_id",
            "input_property_order", "output_properties"]

    def execute(self, net, color_descriptor, active_tokens, service_interfaces):
        operation_id = self.args["operation_id"]
        parallel_idx = active_tokens[0].data.get('parallel_idx', 0)

        input_property_order = self.args["input_property_order"]
        output_properties = self.args["output_properties"]

        workflow_data = load.extract_data_from_tokens(active_tokens)

        outputs = order_outputs(workflow_data,
                input_property_order, output_properties)

        store.store_outputs(outputs, net, operation_id, parallel_idx)

        output_token = net.create_token(color=color_descriptor.color,
                color_group_idx=color_descriptor.group.idx,
                data={'parallel_idx': parallel_idx})

        return [output_token], defer.succeed(None)
