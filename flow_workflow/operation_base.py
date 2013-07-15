from flow_workflow import io
from flow_workflow.entities import factory
from flow_workflow.entities import log_manager


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


    def save(self):
        factory.store_operation(self.net, self)

    @property
    def as_dict(self):
        return {
            '_class': self.__class__.__name__,
            'child_operation_ids': self.child_operation_ids,
            'input_connections': self.input_connections,
            'log_dir': self.log_dir,
            'name': self.name,
            'operation_id': self.operation_id,
            'output_properties': self.output_properties,
            'parent_operation_id': self.parent_operation_id,
        }

    def _child_id_from(self, name):
        return self.child_operation_ids[name]

    def child_named(self, name):
        return factory.load_operation(net=self.net,
                operation_id=self._child_id_from(name))

    @property
    def parent(self):
        return factory.load_operation(net=self.net,
                operation_id=self.parent_operation_id)

    @property
    def log_manager(self):
        return log_manager.LogManager(operation_name=self.name,
                operation_id=self.operation_id, log_dir=self.log_dir)

    @property
    def input_names(self):
        result = []
        for props in self.input_connections.itervalues():
            result.extend(props.keys())
        return result

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
    def __init__(self):
        pass

    def save(self):
        pass


class ConnectorOperation(Operation):
    def load_output(self, name, parallel_id):
        return self.load_input(self.net, name, parallel_id)


class ModelOperation(Operation):
    def __init__(self, *args, **kwargs):
        Operation.__init__(self, *args, **kwargs)
        self._output_connector = None

    def output_connector(self):
        if self._output_connector is None:
            self._output_connector = self.child_named('output connector')

        return self._output_connector

    def load_output(self, name, parallel_id):
        self.output_connector.load_output(self.net, name, parallel_id)
