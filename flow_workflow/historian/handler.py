from flow import exit_codes
from flow.configuration.settings.injector import setting
from flow.handler import Handler
from flow.util.exit import exit_process
from flow_workflow.historian import messages
from flow_workflow.historian.operation_data import OperationData
from flow_workflow.historian.oracle_exceptions import EXIT_ON
from flow_workflow.historian.status import Status
from flow_workflow.historian.storage import WorkflowHistorianStorage
from injector import inject
from twisted.internet import defer

import logging


LOG = logging.getLogger(__name__)


@inject(storage=WorkflowHistorianStorage)
class HistorianHandlerBase(Handler):
    def _handle_message(self, message):
        try:
            self._handle_historian_message(message)
            return defer.succeed(None)

        except EXIT_ON:
            LOG.exception("This historian cannot handle messages anymore, "
                    "because it lost access to Oracle... exiting.")
            exit_process(exit_codes.EXECUTE_FAILURE)


@inject(queue_name=setting('workflow.historian.update_queue'))
class HistorianUpdateHandler(HistorianHandlerBase):
    message_class = messages.UpdateMessage

    def _handle_historian_message(self, message):
        message_dict = self._get_message_dict(message)
        LOG.debug("Updating [net_key='%s', operation_id='%s']: %r",
                message.operation_data['net_key'],
                message.operation_data['operation_id'], message_dict)
        self.storage.update(message_dict)

    def _get_message_dict(self, message):
        message_dict = message.to_dict()

        message_dict['status'] = Status(message_dict['status'])

        OPERATION_DATA_FIELDS = ['operation_data', 'parent_operation_data',
                'peer_operation_data']
        for field in OPERATION_DATA_FIELDS:
            if field in message_dict:
                message_dict[field] = OperationData.from_dict(
                        message_dict[field])

        return message_dict

