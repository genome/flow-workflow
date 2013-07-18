from flow_workflow.future_operation import FutureOperation
import abc
import logging

LOG = logging.getLogger(__name__)


class IAdapter(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def future_net(self, resources):
        raise NotImplementedError()

    @abc.abstractmethod
    def future_operations(self, parent_future_operation,
            input_connections, output_properties):
        raise NotImplementedError()


_NULL_ADAPTER = None


class AdapterBase(IAdapter):
    def __init__(self, operation_id, log_dir=None, parent=_NULL_ADAPTER,
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

    @abc.abstractproperty
    def operation_class(self):
        raise NotImplementedError()

    # Optional overrides
    # ------------------
    @property
    def output_properties(self):
        return []

    @property
    def log_dir(self):
        return self._log_dir


    def future_operation(self, parent_future_operation, input_connections,
            output_properties):
        return FutureOperation(
            operation_class=self.operation_class,
            input_connections=input_connections,
            log_dir=self.log_dir,
            name=self.name,
            operation_id=self.operation_id,
            output_properties=output_properties,
            parent=parent_future_operation)

    def future_operations(self, parent_future_operation, input_connections,
            output_properties):
        return [self.future_operation(parent_future_operation,
            input_connections, output_properties)]


class NullAdapter(AdapterBase):
    operation_class = 'null'

    def __init__(self, operation_id=None):
        AdapterBase.__init__(self, operation_id)

    @property
    def name(self):
        return 'NullAdapter'

    def future_net(self, input_connections, output_properties, resources):
        # XXX Should return a null net?
        pass


_NULL_ADAPTER = NullAdapter()


class XMLAdapterBase(AdapterBase):
    def __init__(self, xml, *args, **kwargs):
        AdapterBase.__init__(self, *args, **kwargs)
        self.xml = xml

    @property
    def name(self):
        return self.xml.attrib['name']

    @property
    def operation_attributes(self):
        return self.xml.attrib

    @property
    def log_dir(self):
        return self._log_dir or self.xml.attrib.get('logDir', '.')

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
