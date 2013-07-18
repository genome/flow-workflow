from flow_workflow import operation_base


class InputConnectorOperation(operation_base.PassThroughOperation):
    def load_input(self, name, parallel_id):
        return self.parent.load_input(name, parallel_id)



class ModelOperation(operation_base.DirectStorageOperation):
    def __init__(self, parallel_by, *args, **kwargs):
        operation_base.DirectStorageOperation.__init__(self, *args, **kwargs)
        self.parallel_by = parallel_by

    @property
    def output_connector(self):
        return self.child_named('output connector')

    def load_output(self, name, parallel_id):
        if self.parallel_by:
            return operation_base.DirectStorageOperation.load_output(self,
                    name, parallel_id)
        else:
            return self.output_connector.load_output(name, parallel_id)
