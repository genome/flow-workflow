from flow import exit_codes
from flow.configuration.settings.injector import setting
from flow.handler import Handler
from flow_workflow.historian.messages import UpdateMessage
from injector import inject
from sqlalchemy.exc import ResourceClosedError, TimeoutError, DisconnectionError
from twisted.internet import defer

import flow.interfaces
import logging
import os


LOG = logging.getLogger(__name__)

@inject(storage=flow.interfaces.IStorage,
        queue_name=setting('workflow.historian.queue'))
class WorkflowHistorianMessageHandler(Handler):
    message_class = UpdateMessage

    def _handle_message(self, message):
        message_dict = message.to_dict()
        LOG.info("Updating [net_key='%s', operation_id='%s']: %r",
                message.net_key, message.operation_id, message_dict)
        try:
            self.storage.update(message_dict)
            return defer.succeed(None)
        except (ResourceClosedError, TimeoutError, DisconnectionError):
            LOG.exception("This historian cannot handle messages anymore because it lost access to Oracle... exiting.")
            os._exit(exit_codes.EXECUTE_FAILURE)

