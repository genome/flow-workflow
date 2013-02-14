from collections import defaultdict
from flow.orchestrator.graph import transitive_reduction
from flow_workflow.nets import GenomeActionNet
from lxml import etree
import flow.petri.netbuilder as nb
import os
import re

MAX_FILENAME_LEN = 50
WORKFLOW_WRAPPER = 'workflow-wrapper'

class WorkflowEntity(object):
    def __init__(self, job_number):
        self.job_number = job_number

    def net(self, builder, input_connections=None):
        raise NotImplementedError("net not implemented in %s" %
                                  self.__class__.__name__)


class WorkflowOperation(WorkflowEntity):
    def __init__(self, job_number, log_dir, xml):
        WorkflowEntity.__init__(self, job_number)
        self.name = xml.attrib["name"]

        self.job_number = job_number

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
        out_file = "%d-%s.out" % (self.job_number, basename)
        err_file = "%d-%s.err" % (self.job_number, basename)
        self.stdout_log_file = os.path.join(self.log_dir, out_file)
        self.stderr_log_file = os.path.join(self.log_dir, err_file)


class CommandOperation(WorkflowOperation):
    def __init__(self, job_number, log_dir, xml):
        WorkflowOperation.__init__(self, job_number, log_dir, xml)
        self.perl_class = self._type_attributes['commandClass']

        self.parallel_by = ""
        if "parallelBy" in self._operation_attributes:
            self.parallel_by = self._operation_attributes["parallelBy"]

    def net(self, builder, input_connections=None):
        if self.parallel_by:
            raise NotImplementedError("No support for parallel_by yet")

        return builder.add_subnet(GenomeActionNet,
                name=self.name,
                job_number=self.job_number,
                action_type="command",
                action_id=self.perl_class,
                input_connections=input_connections)


class EventOperation(WorkflowOperation):
    def __init__(self, job_number, log_dir, xml):
        WorkflowOperation.__init__(self, job_number, log_dir, xml)
        self.event_id = self._type_attributes['eventId']

    def net(self, builder, input_connections=None):
        return builder.add_subnet(GenomeActionNet,
                name=self.name,
                job_number=self.job_number,
                action_type="event",
                action_id=self.event_id,
                input_connections=input_connections)


class ConvergeOperation(WorkflowOperation):
    def __init__(self, job_number, log_dir, xml):
        WorkflowOperation.__init__(self, job_number, log_dir, xml)

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
        raise NotImplementedError("No support for converge node yet")


class InputConnector(WorkflowEntity):
    def __init__(self, job_number):
        WorkflowEntity.__init__(self, job_number)
        self.name = "input connector"

    def net(self, builder, input_connections=None):
        net = builder.add_subnet(nb.EmptyNet, self.name)
        net.start_transition = net.add_transition("input connector start")
        net.start_place = net.add_place("start")
        net.success_transition = net.add_transition("input connector success")
        net.start_transition.arcs_out.add(net.start_place)
        net.start_place.arcs_out.add(net.success_transition)
        return net


class OutputConnector(WorkflowEntity):
    def __init__(self, job_number):
        WorkflowEntity.__init__(self, job_number)
        self.name = "output connector"

    def net(self, builder, input_connections=None):
        net = builder.add_subnet(nb.EmptyNet, self.name)
        net.start_transition = net.add_transition("output connector start")
        net.success_transition = net.add_transition("output connector success")
        net.bridge_transitions(net.start_transition, net.success_transition, "")
        return net


class ModelOperation(WorkflowOperation):
    input_connector_id = 0
    output_connector_id = 1
    first_operation_id = 2

    def __init__(self, job_number, xml, log_dir=None):
        self.operation_types = {
            "Workflow::OperationType::Converge": ConvergeOperation,
            "Workflow::OperationType::Command": CommandOperation,
            "Workflow::OperationType::Model": ModelOperation,
            "Workflow::OperationType::Event": EventOperation,
        }

        log_dir = log_dir or xml.attrib.get("logDir", ".")

        WorkflowOperation.__init__(self, job_number, log_dir, xml)
        self.xml = xml
        self.name = xml.attrib["name"]
        self.job_number = job_number

        self.operations = [
            InputConnector(job_number=self.input_connector_id),
            OutputConnector(job_number=self.output_connector_id),
        ]

        self.edges = {}
        self.input_connections = defaultdict(lambda: defaultdict(dict))

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
        first_op = self.first_operation_id
        self.input_connections[first_op][self.input_connector_id] = {}
        self.add_edge(self.input_connector_id, first_op)
        self.add_edge(first_op, self.output_connector_id)


    def _parse_workflow(self):
        self._parse_operations()
        self._parse_links()

    def _parse_operations(self):
        for operation_node in self.xml.findall("operation"):
            self._add_operation(operation_node)

    def add_edge(self, src_idx, dst_idx):
        if src_idx == dst_idx:
            raise RuntimeError("Attempted to create self cycle with node %d" %
                               src_idx)

        self.edges.setdefault(src_idx, set()).add(dst_idx)

    def _parse_links(self):
        op_indices = dict(((x.name, x.job_number) for x in self.operations))
        for link in self.xml.findall("link"):
            src = link.attrib["fromOperation"]
            dst = link.attrib["toOperation"]

            src_idx = op_indices[src]
            dst_idx = op_indices[dst]

            src_prop = link.attrib["fromProperty"]
            dst_prop = link.attrib["toProperty"]

            self.add_edge(src_idx, dst_idx)

            self.input_connections[dst_idx][src_idx][dst_prop] = src_prop

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
                job_number=idx,
                xml=operation_node,
                log_dir=self.log_dir,
                )
        self.operations.append(operation)

    def net(self, builder, input_connections=None):
        net = builder.add_subnet(nb.SuccessFailureNet, self.name)

        for idx, op in enumerate(self.operations):
            input_conns = self.input_connections.get(idx)
            net.subnets.append(op.net(builder, input_conns))

        net.start.arcs_out.add(net.subnets[0].start_transition)
        net.subnets[self.output_connector_id].success_transition.arcs_out.add(
                net.success)

        net_failure = net.failure

        for idx, subnet in enumerate(net.subnets):
            edges_out = self.edges.get(idx, [])
            if edges_out:
                targets = [net.subnets[i].start_transition for i in edges_out]
                success = subnet.success_transition
                failure = getattr(subnet, "failure_transition", None)
                for tgt in targets:
                    net.bridge_transitions(success, tgt, "")
                    if failure:
                        failure.arcs_out.add(net_failure)

        return net


def convert_workflow_xml(xml_etree):
    return ModelOperation(0, xml_etree)
