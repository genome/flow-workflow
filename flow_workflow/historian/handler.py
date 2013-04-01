import logging
import sys

from sqlalchemy.exc import SQLAlchemyError

from flow_workflow.historian.messages import UpdateMessage

LOG = logging.getLogger(__name__)

class WorkflowHistorianMessageHandler(object):
    message_class = UpdateMessage

    def __init__(self, broker=None, storage=None, queue_name=None):
        self.broker = broker
        self.storage = storage
        self.queue_name = queue_name

    def __call__(self, message):
        message_dict = message.to_dict()
        LOG.info("Updating [net_key='%s', operation_id='%s']: %r",
                message.net_key, message.operation_id, message_dict)
        try:
            self.storage.update(message_dict)
        except SQLAlchemyError:
            LOG.exception("Caught sqlalchemy error in historian handler... exiting.")
            sys._exit()

