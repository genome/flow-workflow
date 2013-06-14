from action_base import TestGenomeActionMixin
from flow_workflow.petri_net.actions import perl_action

import copy
import unittest


class PerlActionTest(TestGenomeActionMixin, unittest.TestCase):
    def setUp(self):
        TestGenomeActionMixin.setUp(self)

        self.args = {
            'operation_id': self.operation_id,
            'action_type': 'command',
            'action_id': 54321,
        }

        self.action = perl_action.GenomeShortcutAction.create(
                self.connection, args=self.args)

    def test_command_line(self):
        token_data = {'parallel_idx': self.parallel_idx}
        command_line = self.action.command_line(self.net, token_data)

        self.assertEqual(map(str, ['flow', 'workflow-wrapper',
                '--action-type', self.args['action_type'],
                '--method', 'shortcut',
                '--action-id', self.args['action_id'],
                '--operation-id', self.operation_id,
                '--parallel-idx', self.parallel_idx]),
            command_line)

    def test_environment(self):
        base_environment = {
            'foo': 'bar',
            'baz': 'buz',
        }
        self.net.constant.return_value = base_environment

        env = self.action.environment(self.net)

        expected_parent_id = '%s %s' % (self.net.key, self.operation_id)
        expected_environment = copy.copy(base_environment)
        expected_environment['FLOW_WORKFLOW_PARENT_ID'] = expected_parent_id

        self.assertEqual(expected_environment, env)


if __name__ == "__main__":
    unittest.main()
