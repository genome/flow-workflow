from flow_workflow import adapter_base
from flow_workflow.pass_through import future_nets


class PassThroughAdapter(adapter_base.AdapterBase):
    def __init__(self, name, operation_class, *args, **kwargs):
        adapter_base.AdapterBase.__init__(self, *args, **kwargs)

        self._name = name
        self._operation_class = operation_class

    @property
    def name(self):
        return self._name

    @property
    def operation_class(self):
        return self._operation_class

    def future_net(self, input_connections, output_properties, resources):
        return future_nets.PassThroughNet(name=self.name,
                operation_id=self.operation_id)
