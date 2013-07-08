import abc


_NULL_OPERATION = None


class OperationBase(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self, operation_id, log_dir='.', parent=_NULL_OPERATION):
        self.operation_id = operation_id
        self._log_dir = log_dir
        self.parent = parent

    # Mandatory overrides
    # -------------------
    @abc.abstractproperty
    def name(self):
        raise NotImplementedError()

    @abc.abstractmethod
    def net(self, input_connections, output_properties, resources):
        raise NotImplementedError()

    # Optional overrides
    # ------------------
    # Are children and operations needed?
    @property
    def operations(self):
        return [self]

    @property
    def children(self):
        return []

    @property
    def output_properties(self):
        return []

class NullOperation(OperationBase):
    def __init__(self, operation_id=None):
        self.operation_id = operation_id

    def name(self):
        return 'NullOperation'

    def net(self, input_connections, output_properties, resources):
        # XXX Should return a null net?
        pass


_NULL_OPERATION = NullOperation()


class AdapterBase(OperationBase):
    def __init__(self, xml, *args, **kwargs):
        OperationBase.__init__(self, *args, **kwargs)
        self.xml = xml

    @property
    def name(self):
        # XXX We should sanitize this, since it's used in redis keys
        return self.xml.attrib['name']

    @property
    def operation_attributes(self):
        return self.xml.attrib

    @property
    def operation_type_node(self):
        type_nodes = self.xml.findall("operationtype")
        if len(type_nodes) != 1:
            raise ValueError(
                "Wrong number of <operationtype> tags in operation %s"
                % self.name)

        return type_nodes[0]

    @property
    def operation_type_attributes(self):
        return self.operation_type_node.attrib
