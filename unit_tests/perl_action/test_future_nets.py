from flow_workflow.future_nets import WorkflowNetBase
from flow_workflow.perl_action import future_nets

import mock
import unittest


class PerlActionNetTest(unittest.TestCase):
    def setUp(self):
        self.name = mock.Mock()
        self.shortcut_action_class = mock.Mock()
        self.execute_action_class = mock.Mock()

        self.action_id = mock.Mock()
        self.action_type = mock.Mock()
        self.operation_id = mock.Mock()
        self.project_name = mock.Mock()
        self.resources = mock.MagicMock()

        self.args = {
            'action_id': self.action_id,
            'action_type': self.action_type,
            'operation_id': self.operation_id,
            'resources': self.resources,
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


if __name__ == '__main__':
    unittest.main()
