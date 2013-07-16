from flow_workflow.clone_inputs_future_net import CloneInputsNet
import flow_workflow.adapter_base


class ConnectorAdapter(flow_workflow.adapter_base.AdapterBase):
    @property
    def name(self):
        return self._name


class InputConnector(ConnectorAdapter):
    operation_class = 'input_connector'

    def __init__(self, name='input connector', *args, **kwargs):
        ConnectorAdapter.__init__(self, *args, **kwargs)
        self._name = name

    def future_net(self, input_connections, output_properties, resources):
        return CloneInputsNet(name=self.name,
                operation_id=self.operation_id,
                input_connections=input_connections)


class OutputConnector(ConnectorAdapter):
    operation_class = 'output_connector'

    def __init__(self, name='output connector', *args, **kwargs):
        ConnectorAdapter.__init__(self, *args, **kwargs)
        self._name = name

    def future_net(self, input_connections, output_properties, resources):
        return CloneInputsNet(name=self.name,
                operation_id=self.parent.operation_id,
                input_connections=input_connections)
