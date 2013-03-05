import logging

LOG = logging.getLogger(__name__)

class WorkflowHistorianMessageHandler(object):
    def __init__(self, broker=None, storage=None, queue_name=None):
        self.broker = broker
        self.storage = storage
        self.queue_name = queue_name

    def __call__(self, message):
        LOG.info("Updating (%s) [net_key='%s', operation_id='%s'] status=%s", 
                message.name, message.net_key, message.operation_id, message.status)

