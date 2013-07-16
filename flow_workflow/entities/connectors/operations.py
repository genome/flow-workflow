from flow_workflow import operation_base


class ConnectorOperation(operation_base.Operation):
    def load_output(self, name, parallel_id):
        return self.load_input(self.net, name, parallel_id)
