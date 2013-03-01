import logging

LOG = logging.getLogger(__name__)

class HistorianMessageHandler(object):
    def __init__(self, broker=None, storage=None, queue_name=None):
        self.broker = broker
        self.storage = storage
        self.queue_name = queue_name

    def __call__(self, message):
        raise NotImplementedError

class CreateOperationMessageHandler(HistorianMessageHandler):
    def __call__(self, message):
        LOG.debug("Creating Operation for key='%s'", message.operation_key)

class UpdateOperationMessageHandler(HistorianMessageHandler):
    def __call__(self, message):
        LOG.debug("Updating Operation for key='%s'", message.operation_key)

