#!/usr/bin/env python

from collections import defaultdict
from flow.orchestrator.graph import transitive_reduction
from lxml import etree
import flow_workflow.nodes as wfnodes
import os
import re
import sys

from flow.orchestrator.types import *

MAX_FILENAME_LEN = 30
WORKFLOW_WRAPPER = 'workflow-wrapper'


class WorkflowEntity(object):
    def __init__(self, job_number):
        self.job_number = job_number

    def node(self, redis, flow_key):
        raise NotImplementedError("node not implemented in %s" %
                                  self.__class__.__name__)


class WorkflowOperation(WorkflowEntity):
    def __init__(self, job_number, log_dir, xml):
        WorkflowEntity.__init__(self, job_number)
        self.name = xml.attrib["name"]
        self.log_dir = log_dir
        self._set_log_files()

        self.job_number = job_number

        type_nodes = xml.findall("operationtype")
        if len(type_nodes) != 1:
            raise RuntimeError(
                "Wrong number of <operationtype> tags in operation %s" % name
            )

        self._type_node = type_nodes[0]
        self._operation_attributes = xml.attrib
        self._type_attributes = self._type_node.attrib

    def _set_log_files(self):
        basename = re.sub("[^A-Za-z0-9_.-]", "_", self.name)[:MAX_FILENAME_LEN]
        out_file = "%d-%s.out" %(self.job_number, basename)
        err_file = "%d-%s.err" %(self.job_number, basename)
        self.stdout_log_file = os.path.join(self.log_dir, out_file)
        self.stderr_log_file = os.path.join(self.log_dir, err_file)


class CommandOperation(WorkflowOperation):
    def __init__(self, job_number, log_dir, xml):
        WorkflowOperation.__init__(self, job_number, log_dir, xml)
        self.perl_class = self._type_attributes['commandClass']

        self.parallel_by = ""
        if "parallelBy" in self._operation_attributes:
            self.parallel_by = self._operation_attributes["parallelBy"]

    def node(self, redis, flow_key):
        if self.parallel_by:
            return wfnodes.ParallelByCommandFlow.create(
                    connection=redis,
                    flow_key=flow_key,
                    perl_class=self.perl_class,
                    stdout_log_file=self.stdout_log_file,
                    stderr_log_file=self.stderr_log_file,
                    parallel_by_property=self.parallel_by,
                    name = self.name,
                    )
        else:
            return wfnodes.CommandNode.create(
                    connection=redis,
                    flow_key=flow_key,
                    perl_class=self.perl_class,
                    stdout_log_file=self.stdout_log_file,
                    stderr_log_file=self.stderr_log_file,
                    name=self.name,
                    )


class EventOperation(WorkflowOperation):
    def __init__(self, job_number, log_dir, xml):
        WorkflowOperation.__init__(self, job_number, log_dir, xml)
        self.event_id = self._type_attributes['eventId']

    def node(self, redis, flow_key):
        return wfnodes.EventNode.create(
                connection=redis,
                flow_key=flow_key,
                name=self.name,
                stdout_log_file=self.stdout_log_file,
                stderr_log_file=self.stderr_log_file,
                event_id=self.event_id)


class ConvergeOperation(WorkflowOperation):
    def __init__(self, job_number, log_dir, xml):
        WorkflowOperation.__init__(self, job_number, log_dir, xml)

        outputs = self._type_node.findall("outputproperty")
        if len(outputs) < 1:
            raise RuntimeError(
                "Wrong number of <outputproperty> tags (%d) in operation %s" %
                (len(outputs), self.name))
        self.output_properties = [x.text for x in outputs]

        inputs = self._type_node.findall("inputproperty")
        self.input_properties = [x.text for x in inputs]

    def node(self, redis, flow_key):
        return wfnodes.ConvergeNode.create(
                connection=redis,
                flow_key=flow_key,
                input_property_order=self.input_properties,
                output_properties=self.output_properties,
                name=self.name,
                )


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

        if xml.tag == "operation" and type_class != "Workflow::OperationType::Model":
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
        self.input_connections[self.first_operation_id][self.input_connector_id] = {}
        self.add_edge(self.input_connector_id, self.first_operation_id)
        self.add_edge(self.first_operation_id, self.output_connector_id)


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
            raise RuntimeError(
                    "Wrong number of <operationtype> subtags (%d) in "
                    "operation %s" % (len(optype_tags), name))

        optype = optype_tags[0]
        type_class = optype.attrib["typeClass"]

        if type_class not in self.operation_types:
            raise RuntimeError("Unknown operation type %s in workflow xml" %
                               type_class)

        idx = len(self.operations)
        op = self.operation_types[type_class](
                job_number=idx,
                xml=operation_node,
                log_dir=self.log_dir,
                )
        self.operations.append(op)

    def node(self, redis, flow_key):
        flow = Flow.create(
                connection=redis,
                name=self.name,
                flow_key=flow_key,
                )

        nodes = [x.node(redis, flow.key) for x in self.operations]
        for idx, node in enumerate(nodes):
            if idx in self.edges:
                node.successors = self.edges[idx]

            if idx in self.rev_edges:
                node.indegree = len(self.rev_edges[idx])

        for dst_idx, props in self.input_connections.iteritems():
            props = dict((nodes[k].key, v) for k, v in props.iteritems())
            nodes[dst_idx].input_connections = props

        flow.node_keys = [n.key for n in nodes]

        return flow


class InputConnector(WorkflowEntity):
    def __init__(self, job_number):
        WorkflowEntity.__init__(self, job_number)
        self.name = "input connector"

    def node(self, redis, flow_key):
        return StartNode.create(connection=redis, flow_key=flow_key,
                                name="start node")


class OutputConnector(WorkflowEntity):
    def __init__(self, job_number):
        WorkflowEntity.__init__(self, job_number)
        self.name = "output connector"

    def node(self, redis, flow_key):
        return StopNode.create(connection=redis, flow_key=flow_key,
                               name="stop node")


def convert_workflow_xml(xml_text):
    xml = etree.XML(xml_text)
    return ModelOperation(0, xml)

if __name__ == "__main__":
    import flow.orchestrator.redisom as rom
    import subprocess

    class FakeCommandLineService(object):
        def __init__(self, conn):
            self.conn = conn

        def submit(self, cmdline, return_identifier=None, executor_options=None):
            cmdline = map(str, cmdline)
            print "EXEC", cmdline
            services = {
                    wfnodes.GENOME_SHORTCUT_SERVICE: self,
                    wfnodes.GENOME_EXECUTE_SERVICE: self,
                    }
            rv = subprocess.call(cmdline)
            if rv == 0:
                callback = return_identifier['on_success']
            else:
                callback = return_identifier['on_failure']

            rom.invoke_instance_method(self.conn, callback, services=services,
                                       return_identifier=return_identifier)


    import redis
    import sys
    import fakeredis

    if len(sys.argv) != 2:
        print "Give filename!"
        sys.exit(1)

    xml = etree.XML(open(sys.argv[1]).read())
    inputs = {
        "a": '"BQcKC29wZXJhdGlvbiBB\\n"',
        "b": '"BQcKC29wZXJhdGlvbiBC\\n"',
        "c": '"BQcKC29wZXJhdGlvbiBD\\n"',
        "d": '"BQcKC29wZXJhdGlvbiBE\\n"',
    }
    model = ModelOperation(0, xml)
    p = Parser(xml)
    redis = fakeredis.FakeRedis()
    flow = p.flow(redis)

    #p = Parser(xml, inputs)
    #print p.edges
    print "Ops"
    #redis = redis.Redis()
    #flow = p.flow(redis)
    #services = {
        #wfnodes.GENOME_SHORTCUT_SERVICE: FakeCommandLineService(redis),
        #wfnodes.GENOME_EXECUTE_SERVICE: FakeCommandLineService(redis),
    #}
    #flow.node(0).execute(services)
