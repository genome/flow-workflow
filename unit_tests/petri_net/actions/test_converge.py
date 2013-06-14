from flow_workflow.petri_net.actions import converge

import fakeredis
import mock
import unittest


class OrderOutputsTest(unittest.TestCase):
    def test_order_outputs(self):
        inputs = {
            'foo1': 'f1',
            'foo0': 'f0',
            'foo2': 'f2',
        }

        input_property_order = ['foo0', 'foo1', 'foo2']
        output_properties = ['bar']

        expected_result = {
            'bar': ['f0', 'f1', 'f2'],
        }

        result = converge.order_outputs(inputs,
                input_property_order, output_properties)
        self.assertEqual(expected_result, result)


class ConvergeActionTest(unittest.TestCase):
    def setUp(self):
        self.conn = fakeredis.FakeRedis()

        self.operation_id = 12345

        self.args = {
            'operation_id': self.operation_id,
            'input_property_order': ['b0', 'b1', 'b2', 'b3'],
            'output_properties': ['bar'],
        }
        self.action = converge.GenomeConvergeAction.create(self.conn,
                args=self.args)

        self.parallel_idx = 42
        self.net = mock.Mock()
        self.color_descriptor = mock.Mock()
        self.service_interfaces = {
            'orchestrator': mock.Mock(),
        }

        self.token = mock.MagicMock()
        self.token.data.get.return_value = self.parallel_idx

    def test_converge(self):
        token_data = {
            'b0': mock.Mock(),
            'b1': mock.Mock(),
            'b2': mock.Mock(),
            'b3': mock.Mock(),
        }

        extract_data_from_tokens = mock.Mock()
        extract_data_from_tokens.return_value = token_data
        store_outputs = mock.Mock()
        with mock.patch('flow_workflow.io.load.extract_data_from_tokens',
                new=extract_data_from_tokens):
            with mock.patch('flow_workflow.io.store.store_outputs',
                    new=store_outputs):
                self.action.execute(self.net, self.color_descriptor,
                        [self.token], self.service_interfaces)

        extract_data_from_tokens.assert_called_once_with([self.token])
        expected_data = {
            'bar': [token_data['b%d' % x] for x in xrange(len(token_data))],
        }
        store_outputs.assert_called_once_with(expected_data,
                self.net, self.operation_id, self.parallel_idx)
        self.net.create_token.assert_called_once_with(
                color=self.color_descriptor.color,
                color_group_idx=self.color_descriptor.group.idx,
                data={'parallel_idx': self.parallel_idx})


if __name__ == "__main__":
    unittest.main()
