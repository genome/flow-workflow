import logging
from flow_workflow.historian.messages import UpdateMessage

LOG = logging.getLogger(__name__)

class WorkflowHistorianClient(object):
    def __init__(self,
            broker=None,
            exchange=None,
            routing_key=None):
        self.broker = broker
        self.exchange = exchange
        self.routing_key = routing_key

    def update(self, net_key, operation_id,
            name=None,
            status=None,
            parent_key=None,
            peer_key=None,
            parallel_index=None,
            xml=None,
            is_subflow=False,
            start_time=None,
            end_time=None,
            stdout=None,
            stderr=None,
            exit_code=None):

        message = UpdateMessage(net_key=net_key, operation_id=operation_id,
                name=name, status=status) #TODO
        self.broker.publish(self.exchange, self.routing_key, message)
