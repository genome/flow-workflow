from flow import exit_codes
from flow_workflow.historian.messages import UpdateMessage
from injector import inject, Setting
from sqlalchemy.exc import ResourceClosedError, TimeoutError, DisconnectionError

import flow.interfaces
import logging
import os


LOG = logging.getLogger(__name__)

@inject(broker=flow.interfaces.IBroker, storage=flow.interfaces.IStorage,
        queue_name=Setting('workflow.historian.queue'))
class WorkflowHistorianMessageHandler(object):
    message_class = UpdateMessage

    def __call__(self, message):
        message_dict = message.to_dict()
        LOG.info("Updating [net_key='%s', operation_id='%s']: %r",
                message.net_key, message.operation_id, message_dict)
        try:
            self.storage.update(message_dict)
        except (ResourceClosedError, TimeoutError, DisconnectionError):
            LOG.exception("This historian cannot handle messages anymore because it lost access to Oracle... exiting.")
            os._exit(exit_codes.EXECUTE_FAILURE)

