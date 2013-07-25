from flow.petri_net import future
from flow_workflow.entities.workflow.actions import NotificationAction
from flow_workflow.historian.actions import UpdateOperationStatus


class WorkflowNet(future.FutureNet):
    def __init__(self, child_net):
        future.FutureNet.__init__(self)
        self.child_net = child_net

        self.start_place = self.add_place('start')
        self.subnets.add(child_net)

        self.start_place.add_arc_out(child_net.start_transition)

        self.observe_transition(child_net.start_transition,
                observer_action=future.FutureAction(UpdateOperationStatus,
                name='update_historian',
                    operation_id=child_net.operation_id, status='new'))
        self.notify_success_transition = self.add_basic_transition(
                name='notify_success_transition',
                action=future.FutureAction(
                    cls=NotificationAction, status='success'))

        self.notify_failure_transition = self.add_basic_transition(
                name='notify_failure_transition',
                action=future.FutureAction(
                    cls=NotificationAction, status='failure'))

        self.bridge_transitions(child_net.success_transition,
                self.notify_success_transition,
                name='notify_success_place')

        self.bridge_transitions(child_net.failure_transition,
                self.notify_failure_transition,
                name='notify_failure_place')

