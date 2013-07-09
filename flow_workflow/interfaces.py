from abc import ABCMeta, abstractmethod

class IWorkflowHistorian(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def update(self, net_key, operation_id, name, workflow_plan_id, **kwargs):
        pass

class IWorkflowCompletion(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def notify(self, net, status):
        pass
