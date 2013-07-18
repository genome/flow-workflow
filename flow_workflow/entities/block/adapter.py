from flow_workflow.pass_through_net import PassThroughNet
import flow_workflow.adapter_base


class BlockAdapter(flow_workflow.adapter_base.XMLAdapterBase):
    operation_class = 'block'


    def future_net(self, input_connections, output_properties, resources):
        return PassThroughNet(name=self.name, operation_id=self.operation_id,
                input_connections=input_connections,
                parent_operation_id=self.parent.operation_id)
