from flow_workflow.operations.pass_thru_adapter import PassThruAdapter
from flow_workflow.operations.block.future_net import BlockNet

import logging


LOG = logging.getLogger(__name__)

class BlockAdapter(PassThruAdapter):
    net_class = BlockNet
