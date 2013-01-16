#!/usr/bin/env python

from flow.orchestrator.types import NodeBase, Flow, StartNode, StopNode, Status
import flow.orchestrator.redisom as rom
import json

MAX_FILENAME_LEN = 30
WORKFLOW_WRAPPER = 'workflow-wrapper'

GENOME_SHORTCUT_SERVICE = 'genome_shortcut'
GENOME_EXECUTE_SERVICE = 'genome_execute'

class LoggingNodeBase(NodeBase):
    stdout_log_file = rom.Property(rom.Scalar)
    stderr_log_file = rom.Property(rom.Scalar)


class ParallelByCommandFlow(Flow):
    perl_class = rom.Property(rom.Scalar)
    parallel_by_property = rom.Property(rom.Scalar)
    stdout_log_file = rom.Property(rom.Scalar)
    stderr_log_file = rom.Property(rom.Scalar)

    def _create_child_node(self, index, **kwargs):
        name = str(self.name) + " (#%d)" % index
        return ParallelByCommandChildNode.create(
                connection=self._connection,
                flow_key=self.key,
                name=name,
                perl_class=self.perl_class,
                parallel_by_property=self.parallel_by_property,
                parallel_by_index=index,
                **kwargs
                )

    def _build_parallel_flow(self):
        num_nodes = len(self.inputs[str(self.parallel_by_property)])

        start_node = StartNode.create(
                connection=self._connection,
                name=str(self.name) + " (start node)",
                flow_key=self.key)

        stop_node = StopNode.create(
                connection=self._connection,
                name=str(self.name) + " (stop node)",
                flow_key=self.key,
                indegree=num_nodes)

        self.node_keys = [start_node.key, stop_node.key]
        stop_node_index = 1

        for i in xrange(num_nodes):
            out = "%s.%d" % (str(self.stdout_log_file), i)
            err = "%s.%d" % (str(self.stderr_log_file), i)
            input_connections = {start_node.key: {}}
            node = self._create_child_node(
                i, stdout_log_file=out, stderr_log_file=err,
                input_connections=input_connections)
            node.indegree = 1

            # list append returns the new size of the list, so
            # the index is that minus one
            node_index = self.node_keys.append(node.key) - 1
            start_node.successors.add(node_index)
            node.successors.add(stop_node_index)

        return start_node

    def _execute(self, services):
        start_node = self._build_parallel_flow()
        start_node.execute(services)


class OperationNodeBase(LoggingNodeBase):
    def _return_identifier(self, method):
        success_callback = self.method_descriptor("on_%s_success" % method)
        failure_callback = self.method_descriptor("on_%s_failure" % method)
        return {
            "node_key": self.key,
            "method": method,
            "on_success": success_callback,
            "on_failure": failure_callback,
        }

    def _command_line(self, method):
        raise NotImplementedError("_command_line not implemented in %s" %
                self.__class__.__name__)

    def _submit_cmdline(self, method, service):
        return_identifier = self._return_identifier(method)
        env = self.flow.environment.value
        env['WORKFLOW_RETURN_IDENTIFIER'] = json.dumps(return_identifier)
        env['WORKFLOW_ROUTING_KEY_SUCCESS'] = _success_routing_key(method)
        env['WORKFLOW_ROUTING_KEY_FAILURE'] = _failure_routing_key(method)

        executor_options = {
                "environment": env,
                "user_id": self.flow.user_id.value,
                "working_directory": self.flow.working_directory.value,
                "stdout": str(self.stdout_log_file),
                "stderr": str(self.stderr_log_file),
                }

        mail_user = env.get('FLOW_MAIL_USER')
        if mail_user:
            executor_options["mail_user"] = mail_user

        print "%s submits command with method %s" % (self.name, method)
        service.submit(
                self._command_line(method),
                return_identifier=return_identifier,
                **executor_options)

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


class CommandNode(OperationNodeBase):
    perl_class = rom.Property(rom.Scalar)

    def _command_line(self, method):
        cmd = map(str, [WORKFLOW_WRAPPER, "command", method, self.perl_class,
                  self.key])

        if method == "execute":
            cmd.append("--reply")
        return cmd


class EventNode(OperationNodeBase):
    event_id = rom.Property(rom.Scalar)

    def _command_line(self, method):
        cmd = [WORKFLOW_WRAPPER, "event", method, self.event_id, self.key]
        if method == "execute":
            cmd.append("--reply")
        return cmd


class ParallelByCommandChildNode(CommandNode):
    parallel_by_property = rom.Property(rom.Scalar)
    parallel_by_index = rom.Property(rom.Scalar)

    def _command_line(self, method):
        cmd = [WORKFLOW_WRAPPER, "command", method, self.perl_class, self.key,
               "--parallel-by", self.parallel_by_property,
               "--parallel-by-index", self.parallel_by_index]
        if method == "execute":
            cmd.append("--reply")
        return cmd


class ConvergeNode(NodeBase):
    output_properties = rom.Property(rom.List)
    input_property_order = rom.Property(rom.List)

    def execute(self, services):
        inputs = self.inputs
        out = [inputs[x] for x in self.input_property_order]
        for prop in self.output_properties:
            self.outputs[prop] = out

        self.complete(services)


def _success_routing_key(method):
    return 'genome.%s.success' % method

def _failure_routing_key(method):
    return 'genome.%s.failure' % method
