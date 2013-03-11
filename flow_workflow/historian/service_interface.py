import logging
from flow_workflow.historian.messages import UpdateMessage

LOG = logging.getLogger(__name__)

class WorkflowHistorianServiceInterface(object):
    def __init__(self,
            broker=None,
            exchange=None,
            routing_key=None):
        self.broker = broker
        self.exchange = exchange
        self.routing_key = routing_key

    def update(self, net_key, operation_id, **kwargs):
        message = UpdateMessage(net_key=net_key, operation_id=operation_id,
                **kwargs)
        self.broker.publish(self.exchange, self.routing_key, message)
