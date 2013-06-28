from flow_workflow.operations.pass_thru_net import PassThruNet

import logging


LOG = logging.getLogger(__name__)

class InputConnectorNet(PassThruNet):
    _name = 'input-connector'

class OutputConnectorNet(PassThruNet):
    _name = 'output-connector'

    @property
    def _action_arg_operation_id(self):
        return self.parent_operation_id
