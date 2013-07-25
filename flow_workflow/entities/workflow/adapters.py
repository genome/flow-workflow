from flow_workflow.entities.workflow.future_nets import WorkflowNet
from flow_workflow.parallel_id import ParallelIdentifier
from flow_workflow.adapter_base import AdapterBase
import flow_workflow.factory


class InputStorageAdapter(AdapterBase):
    operation_class = 'direct_storage'

    def future_net(self, resources):
        raise RuntimeError('InputStorageAdapter has no future net')

    @property
    def name(self):
        return 'InputStorage'


class WorkflowAdapter(AdapterBase):
    operation_class = 'null'

    def __init__(self, xml, inputs, local_workflow=False):
        self.xml = xml
        self.inputs = inputs
        self.local_workflow = local_workflow

        self.inputs_storage_adapter = flow_workflow.factory.adapter(
                'input_storage')
        self._future_net = None
        self._child_adapter = None

    @property
    def name(self):
        return 'Workflow'

    def store_inputs(self, net):
        input_storage_operation = flow_workflow.factory.load_operation(net,
                self.inputs_storage_adapter.operation_id)
        input_storage_operation.store_outputs(self.inputs,
                parallel_id=ParallelIdentifier())

    @property
    def input_connections(self):
        return {
            self.inputs_storage_adapter.operation_id:
                {name: name for name, value in self.inputs.iteritems()}
        }

    @property
    def output_properties(self):
        return self.child_adapter.output_properties

    @property
    def child_adapter(self):
        if not self._child_adapter:
            self._child_adapter = flow_workflow.factory.adapter_from_xml(
                    self.xml, parent=flow_workflow.factory.adapter('null'),
                local_workflow=self.local_workflow)
        return self._child_adapter

    def child_adapter_future_net(self, resources):
        return self.child_adapter.future_net(resources)

    def future_net(self, resources):
        return WorkflowNet(self.child_adapter_future_net(resources))

    def future_operation(self, parent_future_operation, input_connections,
            output_properties):
        return parent_future_operation

    def future_operations(self, parent_future_operation,
            input_connections, output_properties):
        return self.inputs_storage_adapter.future_operations(
                self.future_operation(parent_future_operation,
                    input_connections, output_properties),
                input_connections,
                output_properties
                    ) + self.child_adapter.future_operations(
                            self.future_operation(parent_future_operation,
                                input_connections, output_properties),
                            self.input_connections,
                            self.output_properties)
