import logging

LOG = logging.getLogger(__name__)

class WorkflowHistorianMessageHandler(object):
    def __init__(self, broker=None, storage=None, queue_name=None):
        self.broker = broker
        self.storage = storage
        self.queue_name = queue_name

    def __call__(self, message):
        LOG.info("Updating [net_key='%s', operation_id='%s']",
                message.net_key, message.operation_id)
        self.storage.update(**message.to_dict())

