from flow_workflow.operations import base
from flow_workflow.operations.converge.future_nets import ConvergeNet


class ConvergeAdapter(base.AdapterBase):
    def net(self, input_connections, output_properties, resources):
        return ConvergeNet(name=self.name,
                operation_id=self.operation_id,
                input_connections=input_connections,
                output_properties=self.output_properties,
                resources=resources,
                input_property_order=self.input_property_order,
                parent_operation_id=self.parent.operation_id)

    @property
    def input_property_order(self):
        inputs = self.operation_type_node.findall('inputproperty')
        if len(inputs) < 1:
            raise ValueError(
                "Wrong number of <inputproperty> tags (%d) in operation %s" %
                (len(inputs), self.name))
        return [x.text for x in inputs]

    @property
    def output_properties(self):
        return [o.text for o in
                self.operation_type_node.findall('outputproperty')]
