#!/usr/bin/env python

import json
import re
from flow.orchestrator.types import *
from flow.orchestrator.redisom import *

MAX_FILENAME_LEN = 30
WORKFLOW_WRAPPER = 'workflow-wrapper'

GENOME_SHORTCUT_SERVICE = 'genome_shortcut'
GENOME_EXECUTE_SERVICE = 'genome_execute'

class LoggingNodeBase(NodeBase):
    stdout_log_file = RedisScalar
    stderr_log_file = RedisScalar


class ParallelByCommandFlow(Flow):
    perl_class = RedisScalar
    parallel_by_property = RedisScalar
    stdout_log_file = RedisScalar
    stderr_log_file = RedisScalar

    def _create_child_node(self, index, **kwargs):
        name = self.name.value + " (#%d)" % index
        return ParallelByCommandChildNode.create(
                connection=self._connection,
                flow_key=self.key,
                name=name,
                perl_class=self.perl_class,
                parallel_by_property=self.parallel_by_property,
                parallel_by_index=index,
                **kwargs
                )

    def _execute(self, services):
        inputs = self.inputs
        num_nodes = len(inputs[self.parallel_by_property.value])
        name_base = self.name

        start_node = StartNode.create(
                connection=self._connection,
                name=self.name.value + " (start node)",
                flow_key=self.key,
                outputs=json.dumps(inputs))

        stop_node = StopNode.create(
                connection=self._connection,
                name=self.name.value + " (stop node)",
                flow_key=self.key,
                indegree=num_nodes)

        self.node_keys = [start_node.key, stop_node.key]
        start_node_index = 0
        stop_node_index = 1

        for i in xrange(num_nodes):
            out = "%s.%d" % (self.stdout_log_file.value, i)
            err = "%s.%d" % (self.stderr_log_file.value, i)
            input_connections = {start_node_index: json.dumps({})}
            node = self._create_child_node(
                i, stdout_log_file=out, stderr_log_file=err,
                input_connections=input_connections)
            node.indegree = 1

            # list append returns the new size of the list, so
            # the index is that minus one
            node_index = self.node_keys.append(node.key) - 1
            start_node.successors.add(node_index)
            node.successors.add(stop_node_index)

        start_node.execute(services)


class CommandNode(LoggingNodeBase):
    perl_class = RedisScalar

    def _command_line(self, method):
        cmd = [WORKFLOW_WRAPPER, "command", method, self.perl_class, self.key]
        if method == "execute":
            cmd.append("--reply")
        return cmd

    def _return_identifier(self, method):
        success_callback = self.method_descriptor("on_%s_success" % method)
        failure_callback = self.method_descriptor("on_%s_failure" % method)
        return {
            "node_key": self.key,
            "method": method,
            "on_success": success_callback,
            "on_failure": failure_callback,
        }

    def _submit_cmdline(self, method, service):
        return_identifier = self._return_identifier(method)
        environment = self.flow.environment.value
        environment['WORKFLOW_RETURN_IDENTIFIER'] = json.dumps(return_identifier)
        environment['WORKFLOW_ROUTING_KEY_SUCCESS'] = _success_routing_key(method)
        environment['WORKFLOW_ROUTING_KEY_FAILURE'] = _failure_routing_key(method)

        executor_options = {
            "environment": environment,
            "user_id": self.flow.user_id.value,
            "working_directory": self.flow.working_directory.value,
            "stdout": self.stdout_log_file.value,
            "stderr": self.stderr_log_file.value,
        }

        print "%s submits command with method %s" % (self.name, method)
        service.submit(
            self._command_line(method),
            return_identifier=return_identifier,
            **executor_options
            )

    def _execute(self, services):
        self._submit_cmdline("shortcut", services[GENOME_SHORTCUT_SERVICE])
        self.status = Status.dispatched

    def on_shortcut_failure(self, services):
        print "Shortcut failure for %s" % self.name
        self._submit_cmdline("execute", services[GENOME_EXECUTE_SERVICE])

    def on_shortcut_success(self, services):
        print "Shortcut success for %s" % self.name
        self.on_success(services)

    def on_execute_success(self, services):
        print "Execute success for %s" % self.name
        self.on_success(services)

    def on_execute_failure(self, services):
        print "Execute failure for %s" % self.name
        self.status = Status.failure
        self.fail(services)

    def on_success(self, services):
        self.status = Status.success
        self.complete(services)


class ParallelByCommandChildNode(CommandNode):
    parallel_by_property = RedisScalar
    parallel_by_index = RedisScalar

    def _command_line(self, method):
        cmd = [WORKFLOW_WRAPPER, "command", method, self.perl_class, self.key,
               "--parallel-by", self.parallel_by_property,
               "--parallel-by-index", self.parallel_by_index]
        if method == "execute":
            cmd.append("--reply")
        return cmd


class ConvergeNode(NodeBase):
    output_property = RedisScalar
    input_property_order = RedisList

    def execute(self):
        inputs = self.inputs
        out = [inputs[x] for x in self.input_property_order]
        self.outputs[self.output_property] = json.dumps(out)


def _success_routing_key(method):
    return 'genome.%s.success' % method

def _failure_routing_key(method):
    return 'genome.%s.failure' % method
