from flow_workflow.historian.service_interface import WorkflowHistorianServiceInterface

import flow_workflow.interfaces
import injector

class WorkflowHistorianConfiguration(injector.Module):
    def configure(self, binder):
        binder.bind(flow_workflow.interfaces.IWorkflowHistorian,
                WorkflowHistorianServiceInterface)
