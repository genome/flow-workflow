import abc


_NULL_OPERATION = None


class AdapterBase(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self, operation_id, log_dir=None, parent=_NULL_OPERATION,
            local_workflow=False):
        self.operation_id = operation_id
        self._log_dir = log_dir
        self.parent = parent
        self.local_workflow = local_workflow

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


class NullOperation(AdapterBase):
    def __init__(self, operation_id=None):
        AdapterBase.__init__(self, operation_id)

    def name(self):
        return 'NullOperation'

    def net(self, input_connections, output_properties, resources):
        # XXX Should return a null net?
        pass


_NULL_OPERATION = NullOperation()


class XMLAdapterBase(AdapterBase):
    def __init__(self, xml, *args, **kwargs):
        AdapterBase.__init__(self, *args, **kwargs)
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

    @property
    def output_properties(self):
        return [o.text for o in
                self.operation_type_node.findall('outputproperty')]
