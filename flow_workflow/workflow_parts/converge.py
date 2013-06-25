from flow_workflow.workflow_parts.base import WorkflowOperation
from flow_workflow.petri_net.future_nets.converge import GenomeConvergeNet

def net_class_kwargs(operation, input_connections, output_properties):
    # determine input_property_order
    inputs = operation._type_node.findall("inputproperty")
    if len(inputs) < 1:
        raise ValueError(
            "Wrong number of <inputproperty> tags (%d) in operation %s" %
            (len(inputs), operation.name))
    input_property_order = [x.text for x in inputs]

    return {
            'name':operation.name,
            'operation_id':operation.operation_id,
            'input_connections':input_connections,
            'input_property_order':input_property_order,
            'output_properties':output_properties,
            'parent_operation_id':operation.parent.operation_id,
            }

class ConvergeOperation(WorkflowOperation):
    def net(self, super_net, input_connections=None, output_properties=None,
            resources=None):
        kwargs = net_class_kwargs(self, input_connections, output_properties,
                resources)
        return super_net.add_subnet(GenomeConvergeNet, **kwargs)
