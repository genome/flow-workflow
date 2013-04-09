from collections import defaultdict
from flow.orchestrator.graph import transitive_reduction
from flow_workflow.nets import GenomeInputConnectorNet
from flow_workflow.nets import GenomeOutputConnectorNet
from flow_workflow.nets import GenomePerlActionNet
from flow_workflow.nets import GenomeEmptyNet
from flow_workflow.nets import GenomeConvergeNet
from flow_workflow.nets import GenomeModelNet
from flow_workflow.nets import GenomeParallelByNet
from flow_workflow.nets import WorkflowHistorianUpdateAction
from flow_workflow.nets import StoreInputsAsOutputsAction
from lxml import etree
from flow import petri
from itertools import chain
import flow.petri.netbuilder as nb
import os
import re

import logging

LOG = logging.getLogger(__name__)

MAX_FILENAME_LEN = 256

class WorkflowEntityFactory(object):
    _instance = None

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = WorkflowEntityFactory()
        return cls._instance

    def __init__(self):
        self.next_operation_id = 0
        self._operation_types = {
            "InputConnector": InputConnector,
            "OutputConnector": OutputConnector,
            "Workflow::OperationType::Converge": ConvergeOperation,
            "Workflow::OperationType::Block": BlockOperation,
            "Workflow::OperationType::Command": CommandOperation,
            "Workflow::OperationType::Model": ModelOperation,
            "Workflow::OperationType::Event": EventOperation,
        }


    def create_from_xml(self, xml, *args, **kwargs):
        optype_tags = xml.findall("operationtype")
        name = xml.attrib["name"]
        if len(optype_tags) != 1:
            raise ValueError(
                    "Wrong number of <operationtype> subtags (%d) in "
                    "operation %s" % (len(optype_tags), name))

        optype = optype_tags[0]
        type_class = optype.attrib["typeClass"]
        return self.create(type_class, *args, xml=xml, **kwargs)

    def create(self, type_class, *args, **kwargs):
        if type_class not in self._operation_types:
            raise ValueError("Unknown operation type %s in workflow xml" %
                               type_class)

        cls = self._operation_types[type_class]
        operation_id = self.next_operation_id
        self.next_operation_id += 1
        return cls(operation_id, *args, **kwargs)



class WorkflowEntity(object):
    def __init__(self, operation_id, parent=None):
        self.operation_id = operation_id
        self.parent = parent
        self.parent_id = self.parent.operation_id if parent else None
        self.notify_historian = True

    def net(self, builder, input_connections=None):
        raise NotImplementedError("net not implemented in %s" %
                                  self.__class__.__name__)

    @property
    def children(self):
        return []

    def historian_info(self):
        if self.notify_historian is not True:
            return None

        stdout = getattr(self, 'stdout_log_file', None)
        stderr = getattr(self, 'stderr_log_file', None)

        info = {'id': self.operation_id,
                'name': self.name,
                'status': 'new',
                'parent_operation_id': self.parent_id,
                'stdout': stdout,
                'stderr': stderr}

        return info



class WorkflowOperation(WorkflowEntity):
    def __init__(self, operation_id, xml, log_dir, parent):
        WorkflowEntity.__init__(self, operation_id, parent)

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
        out_file = "%s.%d.out" % (basename, self.operation_id)
        err_file = "%s.%d.err" % (basename, self.operation_id)
        self.stdout_log_file = os.path.join(self.log_dir, out_file)
        self.stderr_log_file = os.path.join(self.log_dir, err_file)


class CommandOperation(WorkflowOperation):
    def __init__(self, operation_id, xml, log_dir, resources, parent):
        WorkflowOperation.__init__(self, operation_id, xml, log_dir, parent)
        self.perl_class = self._type_attributes['commandClass']

        resource = resources.get(self.name, {})
        self.resources = resource.get("resource")
        self.queue = resource.get("queue")

        self.parallel_by = ""
        if "parallelBy" in self._operation_attributes:
            self.parallel_by = self._operation_attributes["parallelBy"]
            self.notify_historian = False

    def net(self, builder, input_connections=None):

        if self.parallel_by:
            return builder.add_subnet(GenomeParallelByNet,
                    name=self.name,
                    child_base_name=self.name,
                    operation_id=self.operation_id,
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

        return builder.add_subnet(GenomePerlActionNet,
                name=self.name,
                operation_id=self.operation_id,
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
    def __init__(self, operation_id, xml, log_dir, resources, parent):
        WorkflowOperation.__init__(self, operation_id, xml, log_dir, parent)
        self.event_id = self._type_attributes['eventId']

        resource = resources.get(self.name, {})
        self.resources = resource.get("resource")
        self.queue = resource.get("queue")

    def net(self, builder, input_connections=None):
        return builder.add_subnet(GenomePerlActionNet,
                name=self.name,
                operation_id=self.operation_id,
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
    def __init__(self, operation_id, xml, log_dir, resources, parent):
        WorkflowOperation.__init__(self, operation_id, xml, log_dir, parent)

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
                operation_id=self.operation_id,
                parent_operation_id=self.parent_id,
                input_connections=input_connections,
                input_property_order=self.input_properties,
                output_properties=self.output_properties,
                stdout=self.stdout_log_file,
                stderr=self.stderr_log_file
                )


class BlockOperation(WorkflowOperation):
    def __init__(self, operation_id, xml, log_dir, resources, parent):
        WorkflowOperation.__init__(self, operation_id, xml, log_dir, parent)

        properties = self._type_node.findall("property")
        if len(properties) < 1:
            raise ValueError(
                "Wrong number of <property> tags (%d) in operation %s" %
                (len(properties), self.name))
        self.properties = [x.text for x in properties]
        self.notify_historian = False

    def net(self, builder, input_connections=None):
        net = builder.add_subnet(GenomeEmptyNet, self.name, self.operation_id,
                self.parent_id, input_connections)

        args = {"operation_id": self.operation_id,
                "input_connections": input_connections}
        action = nb.ActionSpec(cls=StoreInputsAsOutputsAction, args=args)

        net.start_transition = net.add_transition("block", action=action)
        net.success_transition = net.start_transition

        return net


class InputConnector(WorkflowEntity):
    def __init__(self, operation_id, parent):
        WorkflowEntity.__init__(self, operation_id, parent)
        self.name = "input connector"

    def net(self, builder, input_connections=None):
        return builder.add_subnet(GenomeInputConnectorNet,
                self.name, self.operation_id, self.parent_id)


class OutputConnector(WorkflowEntity):
    def __init__(self, operation_id, workflow_id, parent):
        WorkflowEntity.__init__(self, operation_id, parent)
        self.name = "output connector"
        self.workflow_id = workflow_id

    def net(self, builder, input_connections=None):
        return builder.add_subnet(GenomeOutputConnectorNet,
            self.name, self.operation_id, self.parent_id, input_connections,
            self.workflow_id)


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

    def __init__(self, operation_id, xml, log_dir, resources, parent=None):
        self.resources = resources

        log_dir = log_dir or xml.attrib.get("logDir", ".")

        WorkflowOperation.__init__(self, operation_id, xml, log_dir, parent=parent)
        self.name = xml.attrib["name"]

        self.factory = WorkflowEntityFactory.get_instance()
        self.operations = [
            self.factory.create("InputConnector", parent=self),
            self.factory.create("OutputConnector",
                    workflow_id=self.operation_id, parent=self),
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

        first_op_id = first_op.operation_id
        input_op_id = self.input_connector.operation_id
        self.data_arcs[first_op_id][input_op_id] = {}

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

            LOG.info("Model %s processing link (%s:%d:%s) -> (%s:%d:%s)",
                    self.name, src, src_op.operation_id, src_prop, dst,
                    dst_op.operation_id, dst_prop)

            msg = "Model %s processing link (%s:%s) -> (%s:%s)" % (
                    self.name, src, src_prop, dst, dst_prop)

            self.add_edge(src_op, dst_op)

            dst_id = dst_op.operation_id
            src_id = src_op.operation_id
            self.data_arcs[dst_id][src_id][dst_prop] = src_prop


    def _add_operation(self, operation_node):
        operation = self.factory.create_from_xml(xml=operation_node,
                log_dir=self.log_dir,
                resources=self.resources,
                parent=self
                )

        self.operations.append(operation)

    def net(self, builder, data_arcs=None):
        net = builder.add_subnet(GenomeModelNet, self.name, self.operation_id,
                self.parent_id, data_arcs)
        LOG.info("Model %s adding subnet with data arcs %r", self.name,
                data_arcs)

        ops_to_subnets = {}

        for op in self.operations:
            input_conns = self.data_arcs.get(op.operation_id)
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

class Workflow(object):
    def __init__(self, xml_etree, resources, builder, plan_id):
        self.factory = WorkflowEntityFactory.get_instance()
        self.plan_id = plan_id
        self.builder = builder

        # We have to force the outer xml to be a model.
        # Sometimes, a workflow consists of a single <operation> tag instead of
        # a <workflow> tag
        self.model = self.factory.create("Workflow::OperationType::Model",
                xml_etree, log_dir=None, resources=resources)

        self.outer_net = builder.add_subnet(nb.SuccessFailureNet,
                name=self.model.name)
        self.inner_net = self.model.net(builder=self.outer_net)

        self.connect_start(self.inner_net, self.outer_net)
        self.connect_end(self.inner_net, self.outer_net)
        self.set_variables_and_constants()


    def set_variables_and_constants(self):
        self.builder.constants['workflow_id'] = self.model.operation_id
        self.builder.constants['workflow_plan_id'] = self.plan_id

        parent = os.environ.get('FLOW_WORKFLOW_PARENT_ID')
        if parent:
            LOG.info('Setting parent workflow to %s' % parent)
            parent_net_key, parent_op_id = parent.split(' ')
            self.builder.constants['workflow_parent_net_key'] = parent_net_key
            self.builder.constants['workflow_parent_operation_id'] = int(
                    parent_op_id)

        next_id = self.factory.next_operation_id
        self.builder.variables['workflow_next_operation_id'] = next_id

    def historian_updates(self):
        children = self.model.children
        model_info = self.model.historian_info()
        historian_updates = []

        for child in chain([self.model], children):
            info = child.historian_info()
            if info is None:
                continue
            historian_updates.append(info)

        return historian_updates

    def connect_start(self, inner_net, outer_net):
        preserve_token_action = nb.ActionSpec(
                cls=petri.MergeTokensAction,
                args={'input_type': 'output', 'output_type': 'output'},
                )

        token_saver = outer_net.add_transition("token split",
                action=preserve_token_action)

        outer_net.start.arcs_out.add(token_saver)

        historian_args = {"children_info": self.historian_updates(),
                "net_constants_map": {"user_name": "user_name"}}
        historian_action = nb.ActionSpec(cls=WorkflowHistorianUpdateAction,
                args=historian_args)

        historian_transition = outer_net.add_transition("Historian update",
                action=historian_action)

        outer_net.bridge_transitions(token_saver, historian_transition)
        outer_net.bridge_transitions(token_saver, inner_net.start_transition)

    def connect_end(self, inner_net, outer_net):
        inner_net.success_transition.arcs_out.add(outer_net.success)
        failure = getattr(inner_net, 'failure_transition', None)
        if failure:
            failure.arcs_out.add(outer_net.failure)


def parse_workflow_xml(xml_etree, resources, net_builder, plan_id):
    workflow = Workflow(xml_etree, resources, net_builder, plan_id)
    return workflow.outer_net
