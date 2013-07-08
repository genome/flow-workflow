from flow_workflow.operations import base
from flow_workflow.operations.clone_inputs_future_net import CloneInputsNet


class BlockAdapter(base.AdapterBase):
    def net(self, input_connections, output_properties, resources):
        return CloneInputsNet(name=self.name, operation_id=self.operation_id,
                input_connections=input_connections,
                parent_operation_id=self.parent.operation_id)
