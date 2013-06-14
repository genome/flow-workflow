from flow.petri_net.actions.base import BasicActionBase
from flow_workflow.io import load
from flow_workflow.io import store
from twisted.internet import defer

import logging


LOG = logging.getLogger(__name__)


class LoadDataAction(BasicActionBase):
    required_arguments = ["operation_id"]

    def execute(self, net, color_descriptor, active_tokens, service_interfaces):
        operation_id = self.args["operation_id"]
        parallel_idx = active_tokens[0].data.get('parallel_idx', 0)

        outputs = load.operation_outputs(net, operation_id, parallel_idx)
        data = {
            'parallel_idx': parallel_idx,
            'workflow_data': outputs
        }

        output_token = net.create_token(color=color_descriptor.color,
                color_group_idx=color_descriptor.group.idx,
                data=data)

        return [output_token], defer.succeed(None)


class StoreDataAction(BasicActionBase):
    required_arguments = ["operation_id"]

    def execute(self, net, color_descriptor, active_tokens, service_interfaces):
        operation_id = self.args["operation_id"]
        parallel_idx = active_tokens[0].data.get('parallel_idx', 0)

        workflow_data = load.extract_data_from_tokens(active_tokens)

        LOG.debug("StoreOutputsAction (%s/%d color=%r) storing outputs: %r",
                net.key, operation_id, parallel_idx, workflow_data)

        store.store_outputs(workflow_data, net, operation_id, parallel_idx)

        output_token = net.create_token(color=color_descriptor.color,
                color_group_idx=color_descriptor.group.idx,
                data={'parallel_idx': parallel_idx})

        return [output_token], defer.succeed(None)
