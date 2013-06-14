from action_base import TestGenomeActionMixin
from flow_workflow.petri_net.actions import parallel_by
from flow.petri_net.color import ColorGroup

import mock
import unittest


class ParallelByActionTest(TestGenomeActionMixin, unittest.TestCase):
    def setUp(self):
        TestGenomeActionMixin.setUp(self)

    def test_parallel_by_join(self):
        workflow_data = {
            'foo': ['bar', 'baz', 'buz'],
        }

        token = mock.Mock()

        action = parallel_by.ParallelByJoin.create(self.connection)

        extract_data_from_tokens = mock.Mock()
        extract_data_from_tokens.return_value = workflow_data
        with mock.patch('flow_workflow.io.load.extract_data_from_tokens',
                new=extract_data_from_tokens):
            action.execute(self.net, self.color_descriptor,
                    [token], self.service_interfaces)

        extract_data_from_tokens.assert_called_once_with([token])

        expected_data = {
            'workflow_data': workflow_data
        }
        self.net.create_token.assert_called_once_with(
                color=self.color_descriptor.group.parent_color,
                color_group_idx=
                    self.color_descriptor.group.parent_color_group_idx,
                data=expected_data)

    def test_parallel_by_split(self):
        size = 7

        workflow_data = {
            'parallel_by_size': size,
            'foo': ['bar', 'baz', 'buz'],
        }

        new_color_group = ColorGroup(idx=23,
                parent_color=1023, parent_color_group_idx=16,
                begin=8281, end=8281+size)
        self.net.add_color_group.return_value = new_color_group

        token = mock.Mock()

        action = parallel_by.ParallelBySplit.create(self.connection)

        extract_data_from_tokens = mock.Mock()
        extract_data_from_tokens.return_value = workflow_data
        with mock.patch('flow_workflow.io.load.extract_data_from_tokens',
                new=extract_data_from_tokens):
            action.execute(self.net, self.color_descriptor,
                    [token], self.service_interfaces)

        extract_data_from_tokens.assert_called_once_with([token])

        for i, color in enumerate(new_color_group.colors):
            workflow_data['parallel_by_idx'] = i
            expected_data = {
                'workflow_data': workflow_data
            }

            self.net.create_token.assert_any_call(
                    color=color,
                    color_group_idx=new_color_group.idx,
                    data=expected_data)
        self.assertEqual(size, self.net.create_token.call_count)


if __name__ == "__main__":
    unittest.main()
