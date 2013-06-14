from flow_workflow.petri_net.actions import data

import fakeredis
import mock
import unittest


class DataActionTest(unittest.TestCase):
    def setUp(self):
        self.operation_id = 12345
        self.parallel_idx = 42

        self.args = {'operation_id': self.operation_id}

        self.token = mock.MagicMock()
        self.token.data.get.return_value = self.parallel_idx

        self.net = mock.Mock()
        self.color_descriptor = mock.Mock()
        self.service_interfaces = {
            'orchestrator': mock.Mock(),
        }

        self.conn = fakeredis.FakeRedis()


    def test_load_action(self):
        action = data.LoadDataAction.create(self.conn, args=self.args)

        outputs = {
            'foo': 'bar',
            'baz': ['b', 'u', 'z'],
        }

        operation_outputs = mock.Mock()
        operation_outputs.return_value = outputs
        with mock.patch('flow_workflow.io.load.operation_outputs',
                new=operation_outputs):
            output_tokens, deferred = action.execute(self.net,
                    self.color_descriptor, [self.token],
                    self.service_interfaces)

        expected_data = {
            'parallel_idx': self.parallel_idx,
            'workflow_data': outputs,
        }
        operation_outputs.assert_called_once_with(self.net,
                self.operation_id, self.parallel_idx)
        self.net.create_token.assert_called_once_with(
                color=self.color_descriptor.color,
                color_group_idx=self.color_descriptor.group.idx,
                data=expected_data)
        self.assertEqual(output_tokens, [self.net.create_token.return_value])


    def test_store_action(self):
        action = data.StoreDataAction.create(self.conn, args=self.args)

        outputs = {
            'foo': 'bar',
            'baz': ['b', 'u', 'z'],
        }

        extract_data_from_tokens = mock.Mock()
        extract_data_from_tokens.return_value = outputs
        store_outputs = mock.Mock()
        with mock.patch('flow_workflow.io.load.extract_data_from_tokens',
                new=extract_data_from_tokens):
            with mock.patch('flow_workflow.io.store.store_outputs',
                    new=store_outputs):
                output_tokens, deferred = action.execute(self.net,
                        self.color_descriptor, [self.token],
                        self.service_interfaces)

        store_outputs.assert_called_once_with(outputs, self.net,
                self.operation_id, self.parallel_idx)
        extract_data_from_tokens.assert_called_once_with([self.token])

        expected_data = {'parallel_idx': self.parallel_idx}
        self.net.create_token.assert_called_once_with(
                color=self.color_descriptor.color,
                color_group_idx=self.color_descriptor.group.idx,
                data=expected_data)
        self.assertEqual(output_tokens, [self.net.create_token.return_value])


if __name__ == "__main__":
    unittest.main()
