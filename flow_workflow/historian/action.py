import flow.petri.safenet as sn

import os
import logging
from time import localtime, strftime

LOG = logging.getLogger(__name__)
TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"

class WorkflowHistorianUpdateAction(sn.TransitionAction):
    required_args = ['children_info']
    optional_args = ['token_data_map', 'timestamps', 'shortcut']

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

    def _get_timestamp(self):
        now = self.connection.time()
        # convert (sec, microsec) to floating sec
        now = now[0] + now[1] * 1e-6

        return strftime(TIMESTAMP_FORMAT, localtime(now)).upper()

    def _get_extra_data(self, active_tokens_key):
        extra = {}
        token_data_map = self.args.get('token_data_map')
        if token_data_map:
            tokens = self.tokens(active_tokens_key)
            data = sn.merge_token_data(tokens)
            for k, v in token_data_map.iteritems():
                extra = {v: data[k]}

        timestamps = self.args.get('timestamps', [])
        for t in timestamps:
            extra[t] = self._get_timestamp()

        shortcut = self.args.get('shortcut')
        if shortcut is True and 'dispatch_id' in extra:
            extra['dispatch_id'] = 'p%s' % extra['dispatch_id']

        return extra


    def execute(self, active_tokens_key, net, service_interfaces):
        historian = service_interfaces['workflow_historian']
        net_key = net.key

        extra_data = self._get_extra_data(active_tokens_key)

        for child_info in self.args['children_info']:
            child_info.update(extra_data)

            operation_id = child_info.pop('id', None)
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



