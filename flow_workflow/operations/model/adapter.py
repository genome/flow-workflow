from collections import defaultdict
from flow_workflow.operations.adapter_base import AdapterBase

import logging


LOG = logging.getLogger(__name__)

class ModelAdapter(AdapterBase):
    def __init__(self, adapter_factory, operation_id, xml, log_dir, parent=None):
        log_:wqdir = log_dir or xml.attrib.get("logDir", ".")
        AdapterBase.__init__(self, adapter_factory=adapter_factory,
                operation_id=operation_id, xml=xml, log_dir=log_dir,
                parent=parent)

        self.operations = {}
        self.input_connector = self.adapter_factory.create("InputConnector", parent=self),
        self.output_connector = self.adapter_factory.create("OutputConnector",
                    workflow_id=self.operation_id, parent=self),
        self._add_operation(self.input_connector)
        self._add_operation(self.output_connector)

        self.edges = defaultdict(set)
        self.data_arcs = defaultdict(lambda: defaultdict(dict))
        # self.data_arcs[dst_id][src_id][dst_prop] = src_prop
        # -aka- self.data_arcs[dst_id] = input_connections

        self.optype = xml.find("operationtype")
        type_class = self.optype.attrib["typeClass"]

        if (xml.tag == "operation" and
                type_class != "Workflow::OperationType::Model"):
            first_operation = self._add_operation_from_xml(xml)
            self.add_edge(self.input_connector, first_operation)
            self.add_edge(first_operation, self.output_connector)

            first_operation_id = first_operation.operation_id
            input_operation_id = self.input_connector.operation_id
            self.data_arcs[first_operation_id][input_operation_id] = {}
        else:
            for operation_node in xml.findall("operation"):
                self._add_operation(operation_node)
            self._parse_links()

        #self.edges = transitive_reduction(self.edges)

    def _add_operation_from_xml(self, operation_xml):
        operation = self.adapter_factory.create_from_xml(xml=operation_xml,
                log_dir=self.log_dir,
                parent=self)
        self._add_operation(operation)

    def _add_operation(self, operation):
        self.operations[operation.name] = operation

    def add_edge(self, src_op, dst_op):
        if src_op == dst_op:
            raise RuntimeError("Attempted to create self cycle with node %s" %
                               src_op.name)
        self.edges[src_op].add(dst_op)

    def _parse_links(self):
        operations = self.operations
        for link in self.xml.findall("link"):
            src = link.attrib["fromOperation"]
            dst = link.attrib["toOperation"]

            src_op = operations[src]
            dst_op = operations[dst]

            src_prop = link.attrib["fromProperty"]
            dst_prop = link.attrib["toProperty"]

            LOG.info("Model %s processing link (%s:%d:%s) -> (%s:%d:%s)",
                    self.name, src, src_op.operation_id, src_prop, dst,
                    dst_op.operation_id, dst_prop)

            msg = "Model %s processing link (%s:%s) -> (%s:%s)" % (
                    self.name, src, src_prop, dst, dst_prop)

            self.add_edge(src_op, dst_op)

            dst_id = dst_op.operation_id
            src_id = src_op.operation_id
            self.data_arcs[dst_id][src_id][dst_prop] = src_prop

    def _get_output_properties(self, operation_id):
        input_connections = self.data_arcs[operation_id]
        output_properties = set()
        for mapping in input_connections.values():
            output_properties.update(mapping.keys())
        return output_properties

    @property
    def children(self):
        result = []
        for op in self.operations.values():
            result.append(op)
            result.extend(op.children)
        return result

    def net(self, super_net=None, input_connections=None,
            output_properties=None, resources=None):

        # generate the model_net
        model_net_kwargs = {
                'name':self.name,
                'operation_id':self.operation_id,
                'parent_operation_id':self.parent.operation_id,
                }
        if super_net is None:
            model_net = GenomeNetBase(**model_net_kwargs)
        else:
            model_net = super_net.add_subnet(GenomeNetBase,
                    **model_net_kwargs)

        # generate the subnets
        subnets = {}
        for name, operation in self.operations.iteritems():
            operation_id = operation.operation_id
            if operation is self.input_connector:
                op_input_connections = input_connections
            else:
                op_input_connections = self.data_arcs.get(operation_id)
            op_resources = resources.get(name, {})
            op_output_properties = self._get_output_properties(operation_id)
            subnets[operation] = operation.net(super_net=model_net,
                    input_connections=op_input_connections,
                    output_properties=op_output_properties,
                    resources=op_resources)

        # model_net(start) -> input_connector(start)
        ic_net = subnets[self.input_connector]
        model_net.starting_place = model_net.bridge_transitions(
                model_net.internal_start_transition,
                ic_net.start_transition,
                name='starting')

        # output_connector(success) -> model_net(success)
        oc_net = subnets[self.output_connector]
        model_net.succeeding_place = model_net.bridge_transitions(
                oc_net.success_transition,
                model_net.internal_success_transition,
                name='succeeding')

        for src_op, dst_set in self.edges.iteritems():
            src_net = subnets[src_op]

            for dst_op in dst_set:
                dst_net = subnets[dst_op]

                # operator(success) -> next_operator(start)
                model_net.bridge_transitions(src_net.success_transition,
                        dst_net.start_transition)

                # operator(failure) -> model_net(failure)
                src_net.failure_transition.add_arc_out(
                        model_net.internal_failure_place)

        return model_net
