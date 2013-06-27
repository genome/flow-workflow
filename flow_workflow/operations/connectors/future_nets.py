from flow_workflow.operations.pass_thru_net import PassThruNet

import logging


LOG = logging.getLogger(__name__)

class InputConnectorNet(PassThruNet):
    name = 'input-connector'

class OutputConnectorNet(PassThruNet):
    name = 'output-connector'

    @property
    def _action_arg_operation_id(self):
        self.parent.operation_id
