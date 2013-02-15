from collections import defaultdict
from flow.orchestrator.graph import transitive_reduction
from flow_workflow.dataflow import DataArc, DataArcs
from flow_workflow.nets import GenomeActionNet
from flow_workflow.nets import GenomeConvergeNet
from flow_workflow.nets import GenomeModelNet
from flow_workflow.nets import StoreOutputsAction
from lxml import etree
import flow.petri.netbuilder as nb
import os
import re

MAX_FILENAME_LEN = 50
WORKFLOW_WRAPPER = 'workflow-wrapper'

class WorkflowEntity(object):
    @property
    def id(self):
        return id(self)

    def net(self, builder, input_connections=None):
        raise NotImplementedError("net not implemented in %s" %
                                  self.__class__.__name__)


class WorkflowOperation(WorkflowEntity):
    def __init__(self, log_dir, xml):
        WorkflowEntity.__init__(self)
        self.name = xml.attrib["name"]

        type_nodes = xml.findall("operationtype")
        if len(type_nodes) != 1:
            raise ValueError(
                "Wrong number of <operationtype> tags in operation %s" %
                self.name
            )

        self._type_node = type_nodes[0]
        self._operation_attributes = xml.attrib
        self._type_attributes = self._type_node.attrib

        self.log_dir = log_dir
        basename = re.sub("[^A-Za-z0-9_.-]", "_", self.name)[:MAX_FILENAME_LEN]
        out_file = "%d-%s.out" % (self.id, basename)
        err_file = "%d-%s.err" % (self.id, basename)
        self.stdout_log_file = os.path.join(self.log_dir, out_file)
        self.stderr_log_file = os.path.join(self.log_dir, err_file)


class CommandOperation(WorkflowOperation):
    def __init__(self, log_dir, xml):
        WorkflowOperation.__init__(self, log_dir, xml)
        self.perl_class = self._type_attributes['commandClass']

        self.parallel_by = ""
        if "parallelBy" in self._operation_attributes:
            self.parallel_by = self._operation_attributes["parallelBy"]

    def net(self, builder, input_connections=None):
        if self.parallel_by:
            raise NotImplementedError("No support for parallel_by yet")

        return builder.add_subnet(GenomeActionNet,
                name=self.name,
                operation_id=self.id,
                action_type="command",
                action_id=self.perl_class,
                input_connections=input_connections)


class EventOperation(WorkflowOperation):
    def __init__(self, log_dir, xml):
        WorkflowOperation.__init__(self, log_dir, xml)
        self.event_id = self._type_attributes['eventId']

    def net(self, builder, input_connections=None):
        return builder.add_subnet(GenomeActionNet,
                name=self.name,
                operation_id=self.id,
                action_type="event",
                action_id=self.event_id,
                input_connections=input_connections)


class ConvergeOperation(WorkflowOperation):
    def __init__(self, log_dir, xml):
        WorkflowOperation.__init__(self, log_dir, xml)

        outputs = self._type_node.findall("outputproperty")
        if len(outputs) < 1:
            raise ValueError(
                "Wrong number of <outputproperty> tags (%d) in operation %s" %
                (len(outputs), self.name))
        self.output_properties = [x.text for x in outputs]

        inputs = self._type_node.findall("inputproperty")
        if len(inputs) < 1:
            raise ValueError(
                "Wrong number of <inputproperty> tags (%d) in operation %s" %
                (len(inputs), self.name))

        self.input_properties = [x.text for x in inputs]

    def net(self, builder, input_connections=None):
        return builder.add_subnet(GenomeConvergeNet,
                name=self.name,
                operation_id=self.id,
                input_connections=input_connections,
                input_property_order=self.input_properties,
                output_properties=self.output_properties)


class InputConnector(WorkflowEntity):
    def __init__(self):
        WorkflowEntity.__init__(self)
        self.name = "input connector"

    def net(self, builder, input_connections=None):
        net = builder.add_subnet(nb.EmptyNet, self.name)
        args = {
            "operation_id": self.id,
        }

        action = nb.ActionSpec(
                cls=StoreOutputsAction,
                args=args)
        net.start_transition = net.add_transition("input connector start",
                action=action
                )

        net.success_transition = net.start_transition

        return net


class OutputConnector(WorkflowEntity):
    def __init__(self):
        WorkflowEntity.__init__(self)
        self.name = "output connector"

    def net(self, builder, input_connections=None):
        net = builder.add_subnet(nb.EmptyNet, self.name)
        net.start_transition = net.add_transition("output connector start")
        net.success_transition = net.start_transition
        return net


class ModelOperation(WorkflowOperation):
    input_connector_idx = 0
    output_connector_idx = 1
    first_operation_idx = 2

    def __init__(self, xml, log_dir=None):
        self.operation_types = {
            "Workflow::OperationType::Converge": ConvergeOperation,
            "Workflow::OperationType::Command": CommandOperation,
            "Workflow::OperationType::Model": ModelOperation,
            "Workflow::OperationType::Event": EventOperation,
        }

        log_dir = log_dir or xml.attrib.get("logDir", ".")

        WorkflowOperation.__init__(self, log_dir, xml)
        self.xml = xml
        self.name = xml.attrib["name"]

        self.operations = [
            InputConnector(),
            OutputConnector(),
        ]

        self.edges = {}
        self.data_arcs = DataArcs()

        self.optype = xml.find("operationtype")
        type_class = self.optype.attrib["typeClass"]

        if (xml.tag == "operation" and
                type_class != "Workflow::OperationType::Model"):
            self._parse_workflow_simple()
        else:
            self._parse_workflow()

        self.edges = transitive_reduction(self.edges)
        self.rev_edges = {}
        for src, dst_set in self.edges.iteritems():
            for dst in dst_set:
                self.rev_edges.setdefault(dst, set()).add(src)

    def _parse_workflow_simple(self):
        self._add_operation(self.xml)
        first_op = self.first_operation_idx
        self.add_edge(self.input_connector_idx, first_op)
        self.add_edge(first_op, self.output_connector_idx)

    def _parse_workflow(self):
        self._parse_operations()
        self._parse_links()

    def _parse_operations(self):
        for operation_node in self.xml.findall("operation"):
            self._add_operation(operation_node)

    def add_edge(self, src_op, dst_op):
        if src_op == dst_op:
            raise RuntimeError("Attempted to create self cycle with node %s" %
                               src_op.name)

        self.edges.setdefault(src_op, set()).add(dst_op)

    def _parse_links(self):
        op_map = dict(((x.name, x) for x in self.operations))
        for link in self.xml.findall("link"):
            src = link.attrib["fromOperation"]
            dst = link.attrib["toOperation"]

            src_op = op_map[src]
            dst_op = op_map[dst]

            src_prop = link.attrib["fromProperty"]
            dst_prop = link.attrib["toProperty"]

            self.add_edge(src_op, dst_op)

            arc = DataArc(src_op, src_prop, dst_op, dst_prop)
            self.data_arcs.add(arc)

    def _add_operation(self, operation_node):
        optype_tags = operation_node.findall("operationtype")
        name = operation_node.attrib["name"]
        if len(optype_tags) != 1:
            raise ValueError(
                    "Wrong number of <operationtype> subtags (%d) in "
                    "operation %s" % (len(optype_tags), name))

        optype = optype_tags[0]
        type_class = optype.attrib["typeClass"]

        if type_class not in self.operation_types:
            raise ValueError("Unknown operation type %s in workflow xml" %
                               type_class)

        idx = len(self.operations)
        operation = self.operation_types[type_class](
                xml=operation_node,
                log_dir=self.log_dir,
                )
        self.operations.append(operation)

    def net(self, builder, data_arcs=None):
        net = builder.add_subnet(GenomeModelNet, self.name, self.id, data_arcs)

        ops_to_subnets = {}

        input_connections = self.data_arcs.to_input_hash()

        for op in self.operations:
            input_conns = input_connections.get(op.id)
            subnet = op.net(net, input_conns)
            ops_to_subnets[op] = subnet

        net.bridge_transitions(
            net.start_transition,
            net.subnets[self.input_connector_idx].start_transition)

        net.bridge_transitions(
            net.subnets[self.output_connector_idx].success_transition,
            net.success_transition)

        net_failure_place = net.failure_place

        for idx, subnet in enumerate(net.subnets):
            failure = getattr(subnet, "failure_transition", None)
            if failure:
                failure.arcs_out.add(net_failure_place)

            op = self.operations[idx]
            edges_out = self.edges.get(op, [])
            success = subnet.success_transition

            for dst in edges_out:
                tgt = ops_to_subnets[dst].start_transition
                net.bridge_transitions(success, tgt, "")

        return net


def parse_workflow_xml(xml_etree, net_builder):
    outer_net = net_builder.add_subnet(nb.SuccessFailureNet, "workflow")
    model = ModelOperation(xml_etree)
    outer_net.name = model.name

    inner_net = model.net(outer_net)
    outer_net.start.arcs_out.add(inner_net.start_transition)
    inner_net.success_transition.arcs_out.add(outer_net.success)
    failure = getattr(inner_net, "failure_transition", None)
    if failure:
        failure.arcs_out.add(outer_net.failure)

    return outer_net
