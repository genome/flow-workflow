from flow.handler import Handler
from flow_workflow.interfaces import IWorkflowCompletion
from flow_workflow.messages import NotifyCompletionMessage
from injector import inject
from twisted.internet import defer

import flow.interfaces


@inject(broker=flow.interfaces.IBroker)
class WorkflowCompletionServiceInterface(IWorkflowCompletion):
    def notify(self, net, status):
        message = NotifyCompletionMessage(status=status)

        return self.broker.publish(exchange_name='', routing_key=net.key,
                message=message)


class MonitoringCompletionHandler(Handler):
    message_class = NotifyCompletionMessage

    def __init__(self, queue_name, done_deferred):
        self.done_deferred = done_deferred
        self.queue_name = queue_name

        self.status = None

    def _handle_message(self, message):
        self.status = message.status
        self.done_deferred.callback(True)
        return defer.succeed(None)
