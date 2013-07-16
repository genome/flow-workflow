from flow_workflow.operation_base import NullOperation
from flow_workflow.factory import operation_variable_name


class FutureOperation(object):
    def __init__(self, operation_class, name, operation_id, input_connections,
            output_properties, log_dir, parent):
        self.operation_class = operation_class

        self.name = str(name)
        self.operation_id = int(operation_id)
        self.input_connections = input_connections
        self.output_properties = output_properties
        self.log_dir = log_dir
        self.parent = parent

        self._children = {}

        parent._add_child(self)

    def _add_child(self, child):
        self._children[child.name] = child

    @property
    def child_operation_ids(self):
        return {name: c.operation_id for name, c in self._children.iteritems()}

    @property
    def _operation_variable_name(self):
        return operation_variable_name(self.operation_id)

    def save(self, net):
        net.variables[self._operation_variable_name] = self.as_dict

    @property
    def as_dict(self):
        return {
            '_class': self.operation_class,
            'child_operation_ids': self.child_operation_ids,
            'input_connections': self.input_connections,
            'log_dir': self.log_dir,
            'name': self.name,
            'operation_id': self.operation_id,
            'output_properties': self.output_properties,
            'parent_operation_id': self.parent_operation_id,
        }


class NullFutureOperation(FutureOperation):
    def __init__(self, *args, **kwargs):
        pass

    def _add_child(self, child):
        pass

    @property
    def operation_id(self):
        return None
