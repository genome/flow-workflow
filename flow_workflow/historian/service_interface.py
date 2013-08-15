from flow.configuration.settings.injector import setting
from flow_workflow.historian import messages
from injector import inject
from twisted.internet import defer

import flow.interfaces
import flow_workflow.interfaces
import logging


LOG = logging.getLogger(__name__)


@inject(broker=flow.interfaces.IBroker,
        exchange=setting('workflow.historian.exchange'),
        update_routing_key=setting('workflow.historian.update_routing_key'))
class WorkflowHistorianServiceInterface(
        flow_workflow.interfaces.IWorkflowHistorian):
    def update(self, operation_data, name, workflow_plan_id, **kwargs):
        if workflow_plan_id is None or workflow_plan_id < 0:
            # ignore update (don't even make message)
            LOG.debug("Received negative workflow_plan_id:%s, "
                    "ignoring update (operation_data=%s, name=%s,"
                    "workflow_plan_id=%s, kwargs=%s)",
                    workflow_plan_id, operation_data, name,
                    workflow_plan_id, kwargs)
            return defer.succeed(None)
        else:
            LOG.debug("Sending update (operation_data=%s, name=%s,"
                    "workflow_plan_id=%s, kwargs=%s)",
                    operation_data, name, workflow_plan_id, kwargs)
            message = messages.UpdateMessage(operation_data=operation_data,
                    name=name, workflow_plan_id=workflow_plan_id, **kwargs)
            return self.broker.publish(self.exchange, self.update_routing_key,
                    message)
