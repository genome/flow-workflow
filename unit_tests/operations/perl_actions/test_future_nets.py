from unittest import TestCase
from flow_workflow.operations.workflow_net_base import WorkflowNetBase
from flow_workflow.operations.perl_actions.parallel_by_net import ParallelByNet
from flow_workflow.operations.perl_actions.future_nets import (
        PerlActionNet, CommandNet, EventNet)
from flow_workflow.operations.perl_actions import actions

name = 'test_name'
operation_id = 41
input_connections = object()
output_properties = object()
stderr = object()
stdout = object()
resources = object()
action_id = object()
project_name = 'test_project_name'
parent_operation_id = object()

command_net = CommandNet(name=name, operation_id=operation_id,
        input_connections=input_connections, stderr=stderr,
        output_properties=output_properties,
        stdout=stdout, resources=resources, action_id=action_id,
        project_name=project_name,
        parent_operation_id=parent_operation_id)

event_net = EventNet(name=name, operation_id=operation_id,
        input_connections=input_connections, stderr=stderr,
        output_properties=output_properties,
        stdout=stdout, resources=resources, action_id=action_id,
        project_name=project_name,
        parent_operation_id=parent_operation_id)

class Tests(TestCase):
    def test_command_net(self):
        self.assertIsInstance(command_net, PerlActionNet)
        self.assertEqual(command_net.action_type, 'command')

    def test_event_net(self):
        self.assertIsInstance(event_net, PerlActionNet)
        self.assertEqual(event_net.action_type, 'event')

    def test_perl_action_net_action_args(self):
        net = command_net

        expected_args = {
            'operation_id': operation_id,
            'input_connections': input_connections,
            'action_id': action_id,
            'action_type':net.action_type,
            'stderr': stderr,
            'stdout': stdout,
            'lsf_options':{'project':'test_project_name'},
            'resources': resources,
        }
        action_args = net.execute_net.dispatch_transition.action.args
        for key in expected_args:
            print key
            self.assertEqual(action_args[key], expected_args[key])

        del expected_args['lsf_options']
        net = CommandNet(name=name, operation_id=operation_id,
                input_connections=input_connections, stderr=stderr,
                output_properties=output_properties,
                stdout=stdout, resources=resources, action_id=action_id,
                remote_execute=False,
                project_name=project_name,
                parent_operation_id=parent_operation_id)
        for key in expected_args:
            print key
            self.assertEqual(action_args[key], expected_args[key])

    def test_perl_action_net_inheritance(self):
        self.assertIsInstance(command_net, WorkflowNetBase)

    def test_perl_action_net_connections(self):
        net = command_net

        self.assertIn(net.starting_shortcut_place,
                net.internal_start_transition.arcs_out)
        self.assertIn(net.shortcut_net.start_transition,
                net.starting_shortcut_place.arcs_out)

        self.assertIn(net.succeeding_place,
                net.shortcut_net.success_transition.arcs_out)
        self.assertIn(net.starting_execute_place,
                net.shortcut_net.failure_transition.arcs_out)
        self.assertIn(net.execute_net.start_transition,
                net.starting_execute_place.arcs_out)

        self.assertIn(net.failing_place,
                net.execute_net.failure_transition.arcs_out)
        self.assertIn(net.internal_failure_transition,
                net.failing_place.arcs_out)


class ParallelByNetTests(TestCase):
    def line_of_connections(self, list_of_nodes):
        for i, node in enumerate(list_of_nodes[:-1]):
            next_node = list_of_nodes[i+1]
            print "%s -> %s" % (node.name, next_node.name)
            self.assertIn(next_node, node.arcs_out)

    def test_got_values_from_target_net(self):
        net = ParallelByNet(target_net=event_net,
                parallel_property='test_property')

        self.assertIs(net.operation_id, operation_id)
        self.assertIs(net.parent_operation_id, parent_operation_id)
        self.assertIs(net.input_connections, input_connections)
        self.assertIs(net.output_properties, output_properties)
        self.assertIs(net.resources, resources)

    def test_parallel_by_net_connections(self):
        net = ParallelByNet(target_net=event_net,
                parallel_property='test_property')

        self.line_of_connections([net.internal_start_transition,
            net.starting_split_place,
            net.split_transition,
            net.succeeding_split_place,
            net.target_net.start_transition])

        self.line_of_connections([net.target_net.success_transition,
            net.starting_join_place,
            net.join_transition,
            net.succeeding_join_place,
            net.internal_success_transition])

        self.line_of_connections([net.target_net.failure_transition,
            net.failing_target_place,
            net.target_fail_transition,
            net.failing_place,
            net.internal_failure_transition])
