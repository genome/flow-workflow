from flow_workflow import operation_base


class ModelOperation(operation_base.Operation):
    def __init__(self, *args, **kwargs):
        operation_base.Operation.__init__(self, *args, **kwargs)
        self._output_connector = None

    def output_connector(self):
        if self._output_connector is None:
            self._output_connector = self.child_named('output connector')

        return self._output_connector

    def load_output(self, name, parallel_id):
        self.output_connector.load_output(self.net, name, parallel_id)
