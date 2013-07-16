from flow_workflow.clone_inputs_future_net import CloneInputsNet
import flow_workflow.adapter_base


class BlockAdapter(flow_workflow.adapter_base.XMLAdapterBase):
    operation_class = 'block'


    def net(self, input_connections, output_properties, resources):
        return CloneInputsNet(name=self.name, operation_id=self.operation_id,
                input_connections=input_connections,
                parent_operation_id=self.parent.operation_id)
