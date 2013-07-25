from flow_workflow.entities.converge.future_nets import ConvergeNet
import flow_workflow.adapter_base


class ConvergeAdapter(flow_workflow.adapter_base.XMLAdapterBase):
    operation_class = 'direct_storage'

    def future_net(self, resources):
        return ConvergeNet(name=self.name, operation_id=self.operation_id,
                output_properties=self.output_properties,
                input_property_order=self.input_property_order)

    @property
    def input_property_order(self):
        inputs = self.operation_type_node.findall('inputproperty')
        if len(inputs) < 1:
            raise ValueError(
                "Wrong number of <inputproperty> tags (%d) in operation %s" %
                (len(inputs), self.name))
        return [x.text for x in inputs]
