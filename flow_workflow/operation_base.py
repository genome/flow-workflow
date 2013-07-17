from flow_workflow import io
import flow_workflow.log_manager
import flow_workflow.factory


class Operation(object):
    def __init__(self, net, name, operation_id, input_connections,
            output_properties, log_dir, parent_operation_id,
            child_operation_ids):
        self.net = net

        self.child_operation_ids = child_operation_ids
        self.input_connections = input_connections
        self.log_dir = log_dir
        self.name = name
        self.operation_id = operation_id
        self.output_properties = output_properties
        self.parent_operation_id = parent_operation_id

    def _child_id_from(self, name):
        return self.child_operation_ids[name]

    def child_named(self, name):
        return self._load_operation(self._child_id_from(name))

    @property
    def parent(self):
        return self._load_operation(self.parent_operation_id)

    @property
    def log_manager(self):
        return flow_workflow.log_manager.LogManager(operation_name=self.name,
                operation_id=self.operation_id, log_dir=self.log_dir)

    @property
    def input_names(self):
        result = []
        for props in self.input_connections.itervalues():
            result.extend(props.keys())
        return result

    def _load_operation(self, operation_id):
        return flow_workflow.factory.load_operation(net=self.net,
                operation_id=operation_id)

    def load_inputs(self, parallel_id):
        return {name: self.load_input(name, parallel_id)
                for name in self.input_names}

    def load_outputs(self, parallel_id):
        return {name: self.load_output(name, parallel_id)
                for name in self.output_properties}

    def load_input(self, name, parallel_id):
        return io.load_input(
                input_connections=self.input_connections,
                net=self.net,
                parallel_id=parallel_id,
                property_name=name)

    def load_output(self, name, parallel_id):
        return io.load_output(
                net=self.net,
                operation_id=self.operation_id,
                parallel_id=parallel_id,
                property_name=name)

    def store_outputs(self, outputs, parallel_id):
        for name, value in outputs.iteritems():
            self.store_output(name, value, parallel_id)

    def store_output(self, name, value, parallel_id):
        io.store_output(
                net=self.net,
                operation_id=self.operation_id,
                parallel_id=parallel_id,
                property_name=name,
                value=value)


class NullOperation(Operation):
    def __init__(self, *args, **kwargs):
        pass
