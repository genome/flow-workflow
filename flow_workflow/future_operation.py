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

    def _child_data(self, default_net_key):
        return {name: (c.net_key(default_net_key), c.operation_id)
                for name, c in self._children.iteritems()}

    @property
    def _operation_variable_name(self):
        return operation_variable_name(self.operation_id)

    def net_key(self, default_net_key):
        return default_net_key

    def save(self, net):
        net.variables[self._operation_variable_name] = self.as_dict(
                default_net_key=net.key)

    def as_dict(self, default_net_key):
        result = {
            '_class': self.operation_class,
            'children': self._child_data(default_net_key),
            'name': self.name,
            'operation_id': self.operation_id,
            'parent_operation_id': self.parent.operation_id,
            'parent_net_key': self.parent.net_key(default_net_key),
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


class ForeignFutureOperation(object):
    def __init__(self, operation_id, net_key):
        self.operation_id = operation_id
        self._net_key = net_key

    def net_key(self, default_net_key):
        return self._net_key
