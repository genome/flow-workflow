import logging
from flow_workflow.historian.messages import UpdateMessage
from injector import inject, Setting
import flow.interfaces

LOG = logging.getLogger(__name__)

@inject(broker=flow.interfaces.IBroker,
        exchange=Setting('workflow.historian.exchange'),
        routing_key=Setting('workflow.historian.routing_key'))
class WorkflowHistorianServiceInterface(object):
    def update(self, net_key, operation_id, name, workflow_plan_id, **kwargs):
        if workflow_plan_id < 0:
            # ignore update (don't even make message)
            LOG.debug("Received negative workflow_plan_id:%s, "
                    "ignoring update (net_key=%s, operation_id=%s, name=%s,"
                    "workflow_plan_id=%s, kwargs=%s)",
                    workflow_plan_id, net_key, operation_id, name,
                    workflow_plan_id, kwargs)
        else:
            LOG.debug("Sending update (net_key=%s, operation_id=%s, name=%s,"
                    "workflow_plan_id=%s, kwargs=%s)",
                    net_key, operation_id, name, workflow_plan_id, kwargs)
            message = UpdateMessage(net_key=net_key, operation_id=operation_id,
                    name=name, workflow_plan_id=workflow_plan_id,
                    **kwargs)
            self.broker.publish(self.exchange, self.routing_key, message)
