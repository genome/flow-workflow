from flow import exit_codes
from flow.configuration.settings.injector import setting
from flow.handler import Handler
from flow.util.exit import exit_process
from flow_workflow.historian.messages import UpdateMessage
from injector import inject
from sqlalchemy.exc import ResourceClosedError, TimeoutError, DisconnectionError, DatabaseError
from twisted.internet import defer
from flow_workflow.historian.storage import WorkflowHistorianStorage
from flow_workflow.historian.status import Status
from flow_workflow.historian.operation_data import OperationData

import logging
import os


LOG = logging.getLogger(__name__)

@inject(storage=WorkflowHistorianStorage,
        queue_name=setting('workflow.historian.queue'))
class WorkflowHistorianMessageHandler(Handler):
    message_class = UpdateMessage

    def _handle_message(self, message):
        message_dict = self._get_message_dict(message)
        LOG.info("Updating [net_key='%s', operation_id='%s']: %r",
                message.operation_data['net_key'],
                message.operation_data['operation_id'], message_dict)
        try:
            self.storage.update(message_dict)
            return defer.succeed(None)

        except (ResourceClosedError, TimeoutError, DisconnectionError,
                DatabaseError):
            LOG.exception("This historian cannot handle messages anymore, "
                    "because it lost access to Oracle... exiting.")
            exit_process(exit_codes.EXECUTE_FAILURE)

    def _get_message_dict(self, message):
        message_dict = message.to_dict()

        message_dict['status'] = Status(message_dict['status'])

        OPERATION_DATA_FIELDS = ['operation_data', 'parent_operation_data',
                'peer_operation_data']
        for field in OPERATION_DATA_FIELDS:
            if field in message_dict:
                message_dict[field] = OperationData.from_dict(message_dict[field])

        return message_dict

