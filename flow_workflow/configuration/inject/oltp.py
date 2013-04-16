from flow_workflow.historian.storage import WorkflowHistorianStorage

import flow.interfaces
import injector

class OLTPConfiguration(injector.Module):
    def configure(self, binder):
        binder.bind(flow.interfaces.IStorage, WorkflowHistorianStorage)
