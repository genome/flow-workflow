from flow_workflow.operations.adapter_base import AdapterBase
from flow_workflow.operations.converge.future_net import ConvergeNet

import logging


LOG = logging.getLogger(__name__)

class ConvergeAdapter(AdapterBase):
    def net(self, super_net, input_connections=None, output_properties=None,
            resources=None):

        # determine input_property_order
        inputs = operation._type_node.findall("inputproperty")
        if len(inputs) < 1:
            raise ValueError(
                "Wrong number of <inputproperty> tags (%d) in operation %s" %
                (len(inputs), operation.name))
        input_property_order = [x.text for x in inputs]

        net_class_kwargs = {
                'operation_id':self.operation_id,
                'input_connections':input_connections,
                'input_property_order':input_property_order,
                'output_properties':output_properties,
                'parent_operation_id':self.parent.operation_id,
                }
        return super_net.add_subnet(ConvergeNet, **net_class_kwargs)
