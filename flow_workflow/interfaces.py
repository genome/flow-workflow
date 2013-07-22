from abc import ABCMeta, abstractmethod

class IWorkflowHistorian(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def update(self, operation_data, name, workflow_plan_id, **kwargs):
        pass

    @abstractmethod
    def delete(self, operation_data, workflow_plan_id):
        pass


class IWorkflowCompletion(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def notify(self, net, status):
        pass
