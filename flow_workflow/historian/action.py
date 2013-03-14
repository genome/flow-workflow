import flow.petri.safenet as sn

import os
import logging

LOG = logging.getLogger(__name__)

class WorkflowHistorianUpdateAction(sn.TransitionAction):
    required_args = ['children_info']

    def _set_parent_info(self, net, child_info):
        parent_operation_id = child_info.get('parent_operation_id')
        if parent_operation_id:
            parent_net_key = child_info.get("parent_net_key")
            if parent_net_key is None:
                child_info['parent_net_key'] = net.key
        else: # This is a "top level" workflow
            parent_net_key = net.variable("workflow_parent_net_key")
            parent_operation_id = net.variable("workflow_parent_operation_id")

            if parent_net_key is not None and parent_operation_id is not None:
                child_info['parent_net_key'] = parent_net_key
                child_info['parent_operation_id'] = parent_operation_id
                child_info['is_subflow'] = True
            else:
                LOG.info("Unable to determine parent for action %r",
                        self.args.value)

    def execute(self, active_tokens_key, net, service_interfaces):
        historian = service_interfaces['workflow_historian']
        net_key = net.key
        for child_info in self.args['children_info']:
            operation_id = child_info.pop('id')
            if operation_id is None:
                raise RuntimeError("Null operation id in historian update: %r" %
                        self.args.value)

            child_info['workflow_plan_id'] = net.variable("workflow_plan_id")
            self._set_parent_info(net, child_info)

            parent = os.environ.get("FLOW_WORKFLOW_PARENT_ID")
            LOG.debug("Historian update: (operation=%r, parent=%s), %r",
            operation_id, parent, child_info)

            historian.update(net_key=net_key, operation_id=operation_id,
                    **child_info)



