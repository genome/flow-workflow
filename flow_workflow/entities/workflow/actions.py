from flow.petri_net.actions.base import BasicActionBase


class NotificationAction(BasicActionBase):
    required_args = ['status']

    def execute(self, net, color_descriptor, active_tokens, service_interfaces):
        deferred = service_interfaces['workflow_completion'].notify(
                net, status=self.args['status'])
        return map(net.token, active_tokens), deferred

