from flow_workflow.operations.pass_thru_adapter import PassThruAdapter
from flow_workflow.operations.connectors.future_nets import (InputConnectorNet,
        OutputConnectorNet)

import logging


LOG = logging.getLogger(__name__)

class InputConnectorAdapter(PassThruAdapter):
    net_class = InputConnectorNet

class OutputConnectorAdapter(PassThruAdapter):
    net_class = OutputConnectorNet
