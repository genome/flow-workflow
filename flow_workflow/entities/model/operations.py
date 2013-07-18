from flow_workflow import operation_base


class InputConnectorOperation(operation_base.PassThroughOperation):
    def load_input(self, name, parallel_id):
        return self.parent.load_input(name, parallel_id)



class ModelOperation(operation_base.Operation):
    @property
    def output_connector(self):
        return self.child_named('output connector')

    def load_output(self, name, parallel_id):
        return self.output_connector.load_output(name, parallel_id)

    def store_output(self, name, value, parallel_id):
        return self.output_connector.store_output(name, value, parallel_id)
