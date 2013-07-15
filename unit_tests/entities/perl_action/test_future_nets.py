from flow_workflow.entities.future_nets import WorkflowNetBase
from flow_workflow.entities.perl_action import future_nets

import mock
import unittest


class PerlActionNetTest(unittest.TestCase):
    def setUp(self):
        self.name = mock.Mock()
        self.shortcut_action_class = mock.Mock()
        self.execute_action_class = mock.Mock()

        self.action_id = mock.Mock()
        self.action_type = mock.Mock()
        self.input_connections = mock.MagicMock()
        self.operation_id = mock.Mock()
        self.parent_operation_id = mock.Mock()
        self.project_name = mock.Mock()
        self.resources = mock.MagicMock()
        self.stderr = mock.Mock()
        self.stdout = mock.Mock()

        self.args = {
            'action_id': self.action_id,
            'action_type': self.action_type,
            'input_connections': self.input_connections,
            'operation_id': self.operation_id,
            'parent_operation_id': self.parent_operation_id,
            'resources': self.resources,
            'stderr': self.stderr,
            'stdout': self.stdout,
            'project_name': self.project_name,
        }
        self.net = future_nets.PerlActionNet(name=self.name,
                shortcut_action_class=self.shortcut_action_class,
                execute_action_class=self.execute_action_class,
                **self.args)

    def test_starting_path(self):
        self.assertIn(self.net.starting_shortcut_place,
                self.net.internal_start_transition.arcs_out)
        self.assertIn(self.net.shortcut_net.start_transition,
                self.net.starting_shortcut_place.arcs_out)

    def test_shortcut_success_path(self):
        self.assertIn(self.net.succeeding_place,
                self.net.shortcut_net.success_transition.arcs_out)

        self.assertIn(self.net.internal_success_transition,
                self.net.succeeding_place.arcs_out)

    def test_execute_success_path(self):
        self.assertIn(self.net.starting_execute_place,
                self.net.shortcut_net.failure_transition.arcs_out)
        self.assertIn(self.net.execute_net.start_transition,
                self.net.starting_execute_place.arcs_out)
        self.assertIn(self.net.succeeding_place,
                self.net.execute_net.success_transition.arcs_out)

        self.assertIn(self.net.internal_success_transition,
                self.net.succeeding_place.arcs_out)

    def test_execute_failure_path(self):
        self.assertIn(self.net.starting_execute_place,
                self.net.shortcut_net.failure_transition.arcs_out)
        self.assertIn(self.net.execute_net.start_transition,
                self.net.starting_execute_place.arcs_out)
        self.assertIn(self.net.failing_place,
                self.net.execute_net.failure_transition.arcs_out)

        self.assertIn(self.net.internal_failure_transition,
                self.net.failing_place.arcs_out)

class ParallelByNetTest(unittest.TestCase):
    def setUp(self):
        self.name = mock.Mock()
        self.operation_id = '12345'
        self.parent_operation_id = '54321'
        self.input_connections = mock.Mock()
        self.output_properties = mock.Mock()
        self.resources = mock.Mock()

        self.target_net = WorkflowNetBase(name=self.name,
                operation_id=self.operation_id,
                input_connections=self.input_connections,
                resources=self.resources,
                parent_operation_id=self.parent_operation_id)

        self.parallel_property = 'foo'

        self.net = future_nets.ParallelByNet(target_net=self.target_net,
                parallel_property=self.parallel_property,
                output_properties=self.output_properties)

    def test_start_path(self):
        self.assertIn(self.net.starting_split_place,
                self.net.internal_start_transition.arcs_out)

        self.assertIn(self.net.split_transition,
                self.net.starting_split_place.arcs_out)
        self.assertIn(self.net.succeeding_split_place,
                self.net.split_transition.arcs_out)
        self.assertIn(self.target_net.start_transition,
                self.net.succeeding_split_place.arcs_out)

    def test_success_path(self):
        self.assertIn(self.net.starting_join_place,
                self.target_net.success_transition.arcs_out)
        self.assertIn(self.net.join_transition,
                self.net.starting_join_place.arcs_out)
        self.assertIn(self.net.succeeding_join_place,
                self.net.join_transition.arcs_out)
        self.assertIn(self.net.internal_success_transition,
                self.net.succeeding_join_place.arcs_out)

    def test_failure_path(self):
        self.assertIn(self.net.failing_target_place,
                self.target_net.failure_transition.arcs_out)
        self.assertIn(self.net.target_fail_transition,
                self.net.failing_target_place.arcs_out)
        self.assertIn(self.net.failing_place,
                self.net.target_fail_transition.arcs_out)
        self.assertIn(self.net.internal_failure_transition,
                self.net.failing_place.arcs_out)


if __name__ == '__main__':
    unittest.main()
