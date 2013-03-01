import logging
from flow_workflow.historian.messages import CreateOperationMessage, \
        UpdateOperationMessage

LOG = logging.getLogger(__name__)

class WorkflowHistorianClient(object):
    def __init__(self,
            broker=None,
            exchange=None,
            create_routing_key=None,
            update_routing_key=None):
        self.broker = broker
        self.exchange = exchange
        self.create_routing_key = create_routing_key
        self.update_routing_key = update_routing_key

    def create_operation(self, operation_key, name,
            parent_key=None,
            peer_key=None,
            parallel_index=None,
            xml=None,
            is_subflow=False):

        message = CreateOperationMessage() #TODO
        self.broker.publish(self.exchange, self.update_routing_key, message)

    def update_operation(self, operation_key, status,
            start_time=None,
            end_time=None,
            stdout=None,
            stderr=None,
            exit_code=None):

        message = UpdateOperationMessage() #TODO
        self.broker.publish(self.exchange, self.update_routing_key, message)
