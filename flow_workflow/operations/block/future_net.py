from flow_workflow.operations.pass_thru_net import PassThruNet

import logging


LOG = logging.getLogger(__name__)

class BlockNet(PassThruNet):
    _name = 'block'
