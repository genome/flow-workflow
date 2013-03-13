from collections import defaultdict
from flow.orchestrator.graph import transitive_reduction
from flow_workflow.nets import GenomeActionNet
from flow_workflow.nets import GenomeConvergeNet
from flow_workflow.nets import GenomeModelNet
from flow_workflow.nets import GenomeParallelByNet
from flow_workflow.nets import StoreInputsAsOutputsAction
from flow_workflow.nets import StoreOutputsAction
from flow_workflow.nets import WorkflowHistorianUpdateAction
from lxml import etree
import flow.petri.netbuilder as nb
import flow.petri.safenet as sn
import os
import re

import logging

LOG = logging.getLogger(__name__)

MAX_FILENAME_LEN = 50
WORKFLOW_WRAPPER = 'workflow-wrapper'


class WorkflowEntity(object):
    @property
    def id(self):
        return id(self)

    def __init__(self, parent=None):
        self.parent = parent
        self.parent_id = self.parent.id if parent else None

    def net(self, builder, input_connections=None):
        raise NotImplementedError("net not implemented in %s" %
                                  self.__class__.__name__)

    @property
    def children(self):
        return []


class WorkflowOperation(WorkflowEntity):
    def __init__(self, xml, log_dir, parent):
        WorkflowEntity.__init__(self, parent)

        self.xml = xml
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
    def __init__(self, xml, log_dir, resources, parent):
        WorkflowOperation.__init__(self, xml, log_dir, parent)
        self.perl_class = self._type_attributes['commandClass']

        resource = resources.get(self.name, {})
        self.resources = resource.get("resource")
        self.queue = resource.get("queue")

        self.parallel_by = ""
        if "parallelBy" in self._operation_attributes:
            self.parallel_by = self._operation_attributes["parallelBy"]

    def net(self, builder, input_connections=None):

        if self.parallel_by:
            return builder.add_subnet(GenomeParallelByNet,
                    name=self.name,
                    operation_id=self.id,
                    parent_operation_id=self.parent_id,
                    action_type="command",
                    action_id=self.perl_class,
                    input_connections=input_connections,
                    parallel_by=self.parallel_by,
                    stdout=self.stdout_log_file,
                    stderr=self.stderr_log_file,
                    queue=self.queue,
                    resources=self.resources
                    )

        return builder.add_subnet(GenomeActionNet,
                name=self.name,
                operation_id=self.id,
                parent_operation_id=self.parent_id,
                action_type="command",
                action_id=self.perl_class,
                input_connections=input_connections,
                stdout=self.stdout_log_file,
                stderr=self.stderr_log_file,
                queue=self.queue,
                resources=self.resources
                )


class EventOperation(WorkflowOperation):
    def __init__(self, xml, log_dir, resources, parent):
        WorkflowOperation.__init__(self, xml, log_dir, parent)
        self.event_id = self._type_attributes['eventId']

        resource = resources.get(self.name, {})
        self.resources = resource.get("resource")
        self.queue = resource.get("queue")

    def net(self, builder, input_connections=None):
        return builder.add_subnet(GenomeActionNet,
                name=self.name,
                operation_id=self.id,
                parent_operation_id=self.parent_id,
                action_type="event",
                action_id=self.event_id,
                input_connections=input_connections,
                stdout=self.stdout_log_file,
                stderr=self.stderr_log_file,
                queue=self.queue,
                resources=self.resources
                )


class ConvergeOperation(WorkflowOperation):
    def __init__(self, xml, log_dir, resources, parent):
        WorkflowOperation.__init__(self, xml, log_dir, parent)

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
                parent_operation_id=self.parent_id,
                input_connections=input_connections,
                input_property_order=self.input_properties,
                output_properties=self.output_properties,
                stdout=self.stdout_log_file,
                stderr=self.stderr_log_file
                )


class InputConnector(WorkflowEntity):
    def __init__(self, parent):
        WorkflowEntity.__init__(self, parent)
        self.name = "input connector"

    def net(self, builder, input_connections=None):
        net = builder.add_subnet(nb.EmptyNet, self.name)
        args = {"operation_id": self.id}

        action = nb.ActionSpec(cls=StoreOutputsAction, args=args)
        net.start_transition = net.add_transition("input connector start",
                action=action
                )

        net.success_transition = net.start_transition

        return net


class OutputConnector(WorkflowEntity):
    def __init__(self, workflow_id, parent):
        WorkflowEntity.__init__(self, parent)
        self.name = "output connector"
        self.workflow_id = workflow_id

    def net(self, builder, input_connections=None):
        net = builder.add_subnet(nb.EmptyNet, self.name)

        args = {"operation_id": self.workflow_id,
                "input_connections": input_connections}
        action = nb.ActionSpec(cls=StoreInputsAsOutputsAction, args=args)

        net.start_transition = net.add_transition("output connector start",
                action=action)
        net.success_transition = net.start_transition

        return net


class ModelOperation(WorkflowOperation):
    _input_connector_idx = 0
    _output_connector_idx = 1
    _first_operation_idx = 2

    @property
    def children(self):
        result = []
        for op in self.operations:
            result.append(op)
            result.extend(op.children)
        return result

    @property
    def input_connector(self):
        return self.operations[self._input_connector_idx]

    @property
    def output_connector(self):
        return self.operations[self._output_connector_idx]

    def __init__(self, xml, log_dir, resources, parent=None):
        self.resources = resources

        self.operation_types = {
            "Workflow::OperationType::Converge": ConvergeOperation,
            "Workflow::OperationType::Command": CommandOperation,
            "Workflow::OperationType::Model": ModelOperation,
            "Workflow::OperationType::Event": EventOperation,
        }

        log_dir = log_dir or xml.attrib.get("logDir", ".")

        WorkflowOperation.__init__(self, xml, log_dir, parent=parent)
        self.name = xml.attrib["name"]

        self.operations = [
            InputConnector(parent=self),
            OutputConnector(workflow_id=self.id, parent=self),
        ]

        self.edges = {}
        self.data_arcs = defaultdict(lambda: defaultdict(dict))


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
        first_op = self.operations[self._first_operation_idx]

        self.add_edge(self.input_connector, first_op)
        self.add_edge(first_op, self.output_connector)
        self.data_arcs[first_op.id][self.input_connector.id] = {}

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

            self.data_arcs[dst_op.id][src_op.id][dst_prop] = src_prop


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
                resources=self.resources,
                parent=self
                )
        self.operations.append(operation)

    def net(self, builder, data_arcs=None):
        net = builder.add_subnet(GenomeModelNet, self.name, self.id,
                self.parent_id, data_arcs)

        ops_to_subnets = {}

        for op in self.operations:
            input_conns = self.data_arcs.get(op.id)
            subnet = op.net(net, input_conns)
            ops_to_subnets[op] = subnet

        net.bridge_transitions(
            net.start_transition,
            ops_to_subnets[self.input_connector].start_transition)

        net.bridge_transitions(
            ops_to_subnets[self.output_connector].success_transition,
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


def parse_workflow_xml(xml_etree, resources, net_builder, plan_id):
    outer_net = net_builder.add_subnet(nb.SuccessFailureNet, "workflow")
    model = ModelOperation(xml_etree, log_dir=None, resources=resources)
    outer_net.name = model.name

    token_merger = nb.ActionSpec(
            cls=sn.MergeTokensAction,
            args={"input_type": "output", "output_type": "output"},
            )

    inner_net = model.net(outer_net)
    inner_net.start_transition.action = token_merger

    model_info = {'id': model.id, 'name': model.name, 'status': 'new'}

    children = model.children
    children_info = [{'id': x.id, 'name': x.name, 'status': 'new',
            'parent_operation_id': x.parent.id}
            for x in children]

    historian_info = [model_info] + children_info

    token_split = outer_net.add_transition('token split', action=token_merger)
    outer_net.start.arcs_out.add(token_split)

    phist_in = outer_net.add_place("historian data")
    pinputs_in = outer_net.add_place("input data")

    phist_out = outer_net.add_place("historian data")
    pinputs_out = outer_net.add_place("input data")

    token_split.arcs_out.add(phist_in)
    token_split.arcs_out.add(pinputs_in)

    phist_out.arcs_out.add(inner_net.start_transition)
    pinputs_out.arcs_out.add(inner_net.start_transition)

    action_spec = nb.ActionSpec(cls=WorkflowHistorianUpdateAction,
            args={'children_info': historian_info})

    outer_net.bridge_places(phist_in, phist_out, 'WorkflowHistorianUpdate',
            action_spec)
    outer_net.bridge_places(pinputs_in, pinputs_out, 'Input data',
            token_merger)

    inner_net.success_transition.arcs_out.add(outer_net.success)
    failure = getattr(inner_net, "failure_transition", None)
    if failure:
        failure.arcs_out.add(outer_net.failure)

    net_builder.variables["workflow_id"] = model.id
    net_builder.variables["workflow_plan_id"] = plan_id

    parent = os.environ.get("FLOW_WORKFLOW_PARENT_ID")
    if parent:
        LOG.info("Setting parent workflow to %s" % parent)
        parent_net_key, parent_op_id = parent.split(" ")
        net_builder.variables["workflow_parent_net_key"] = parent_net_key
        net_builder.variables["workflow_parent_operation_id"] = int(parent_op_id)

    return outer_net
