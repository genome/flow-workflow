from flow_workflow.entities import adapter_base
from flow_workflow.entities.clone_inputs_future_net import CloneInputsNet


class ConnectorAdapter(adapter_base.AdapterBase):
    @property
    def name(self):
        return self._name


class InputConnector(ConnectorAdapter):
    def __init__(self, name='input connector', *args, **kwargs):
        ConnectorAdapter.__init__(self, *args, **kwargs)
        self._name = name

    def net(self, input_connections, output_properties, resources):
        return CloneInputsNet(name=self.name,
                operation_id=self.operation_id,
                input_connections=input_connections)


class OutputConnector(ConnectorAdapter):
    def __init__(self, name='output connector', *args, **kwargs):
        ConnectorAdapter.__init__(self, *args, **kwargs)
        self._name = name

    def net(self, input_connections, output_properties, resources):
        return CloneInputsNet(name=self.name,
                operation_id=self.parent.operation_id,
                input_connections=input_connections)
