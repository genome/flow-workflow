from collections import defaultdict
from flow_workflow.operations import base
from flow_workflow.operations import factory
from flow_workflow.operations.model import future_nets


class LinkAdapter(object):
    def __init__(self, xml):
        self.xml = xml

    @property
    def from_operation(self):
        return self.xml.attrib['fromOperation']

    @property
    def to_operation(self):
        return self.xml.attrib['toOperation']

    @property
    def from_property(self):
        return self.xml.attrib['fromProperty']

    @property
    def to_property(self):
        return self.xml.attrib['toProperty']


class ModelAdapter(base.AdapterBase):
    def __init__(self, *args, **kwargs):
        base.AdapterBase.__init__(self, *args, **kwargs)

        self._children = []
        self._child_operation_ids = {}

        self.input_connector = factory.operation('input connector', parent=self)
        self.output_connector = factory.operation('output connector',
                parent=self)

        self._add_child(self.input_connector)
        self._add_child(self.output_connector)

        for operation_xml in self.xml.findall('operation'):
            child = factory.operation_from_xml(operation_xml,
                    log_dir=self.log_dir, parent=self,
                    local_workflow=self.local_workflow)
            self._add_child(child)

        self.links = map(LinkAdapter, self.xml.findall('link'))

    def _add_child(self, child):
        self._children.append(child)
        self._child_operation_ids[child.name] = child.operation_id

    @property
    def output_properties(self):
        results = []
        for link in self.links:
            if link.to_operation == 'output connector':
                results.append(link.to_property)
        return results

    # XXX fix log dir
    @property
    def log_dir(self):
        return self._log_dir or self.xml.attrib.get('logDir', '.')

    @property
    def edges(self):
        edges = defaultdict(set)
        for link in self.links:
            edges[link.from_operation].add(link.to_operation)

        #return transitive_reduction(edges)
        return edges

    @property
    def data_arcs(self):
        # self.data_arcs[dst_id][src_id][dst_prop] = src_prop
        # -aka- self.data_arcs[dst_id] = input_connections
        data_arcs = defaultdict(lambda: defaultdict(dict))

        for link in self.links:
            data_arcs[link.to_operation][link.from_operation]\
                    [link.to_property] = link.from_property

        return data_arcs

    @property
    def children(self):
        return self._children

    def child_operation_id(self, child_name):
        return self._child_operation_ids[child_name]

    @property
    def operations(self):
        result = [self]
        for child in self.children:
            result.extend(child.operations)
        return result

    def child_input_connections(self, child_name, input_connections):
        if child_name == 'input connector':
            return input_connections
        else:
            return self._calculated_child_input_connections(child_name)

    def _calculated_child_input_connections(self, child_name):
        input_connections = defaultdict(dict)
        for link in self.links:
            if link.to_operation == child_name:
                src = self.child_operation_id(link.from_operation)
                input_connections[src][link.to_property] = link.from_property

        return input_connections

    def child_output_properties(self, child_name, output_properties):
        if child_name == 'output connector':
            return output_properties
        else:
            return self._calculated_child_output_properties(child_name)

    def _calculated_child_output_properties(self, child_name):
        output_properties = []
        for link in self.links:
            if link.from_operation == child_name:
                output_properties.append(link.from_property)

        return output_properties

    def subnets(self, input_connections, output_properties, resources):
        child_nets = {}
        for child in self.children:
            child_op = self.child_output_properties(child.name,
                    output_properties)

            child_nets[child.name] = child.net(
                    input_connections=self.child_input_connections(
                        child.name, input_connections),
                    output_properties=self.child_output_properties(
                        child.name, output_properties),
                    resources=resources.get(child.name, {}))

        return child_nets

    def net(self, input_connections, output_properties, resources):
        subnets = self.subnets(input_connections, output_properties,
                resources.get('children', {}))
        return future_nets.ModelNet(subnets=subnets,
                edges=self.edges, name=self.name,
                operation_id=self.operation_id,
                input_connections=input_connections,
                parent_operation_id=self.parent.operation_id)
