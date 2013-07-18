from flow_workflow.operation_base import NullOperation
from flow_workflow.factory import operation_variable_name


class FutureOperation(object):
    def __init__(self, operation_class, operation_id, name, parent, **kwargs):
        self.operation_class = operation_class
        self.operation_id = int(operation_id)
        self.name = name
        self.parent = parent
        self.kwargs = kwargs

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
        result = {
            '_class': self.operation_class,
            'child_operation_ids': self.child_operation_ids,
            'name': self.name,
            'operation_id': self.operation_id,
            'parent_operation_id': self.parent.operation_id,
        }

        result.update(self.kwargs)
        return result


class NullFutureOperation(FutureOperation):
    def __init__(self, *args, **kwargs):
        pass

    def _add_child(self, child):
        pass

    @property
    def operation_id(self):
        return None
