from flow_workflow.workflow_parts.base import WorkflowOperation
from flow_workflow.petri_net.future_nets.block import GenomeBlockNet

def net_class_kwargs(operation):
    return {
            'name':operation.name,
            'operation_id':operation.operation_id,
            'input_connections':input_connections,
            'parent_operation_id':operation.parent.operation_id,
            }

class BlockOperation(WorkflowOperation):
    def net(self, super_net, input_connections=None, output_properties=None,
            resources=None):
        kwargs = net_class_kwargs(self, input_connections, resources)
        return super_net.add_subnet(GenomeBlockNet, **kwargs)
