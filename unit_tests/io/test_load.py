from flow_workflow.io import common
from flow_workflow.io import load

import mock
import unittest


class LoadOperationDataTest(unittest.TestCase):
    def setUp(self):
        self.net = mock.Mock()
        self.operation_id = 12345
        self.parallel_idx = 42

    def test_get_workflow_outputs(self):
        self.net.constant.return_value = self.operation_id
        self.check_operation_outputs()

    def test_operation_outputs(self):
        self.check_operation_outputs()

    def check_operation_outputs(self):
        output_names = ['a', 'b', 'c']

        operation_output_names = mock.Mock()
        operation_output_names.return_value = output_names
        with mock.patch('flow_workflow.io.common.operation_output_names',
                new=operation_output_names):
            results = load.operation_outputs(self.net,
                    self.operation_id, self.parallel_idx)

        operation_output_names.assert_called_once_with(self.net,
                self.operation_id, self.parallel_idx)

        for name in output_names:
            var_name = common.output_variable_name(
                    self.operation_id, name, self.parallel_idx)
            self.net.variable.assert_any_call(var_name)
            self.assertEqual(self.net.variable.return_value, results[name])


class LoadTokenDataTest(unittest.TestCase):
    def setUp(self):
        self.num_tokens = 3
        self.tokens = [mock.MagicMock() for x in xrange(self.num_tokens)]

        for i, t in enumerate(self.tokens):
            t.data.get.return_value = {
                str(i): 'val_%d' % i,
            }

    def test_extract_data_from_tokens(self):
        results = load.extract_data_from_tokens(self.tokens)
        expected_results = {str(i): 'val_%d' % i
                for i in xrange(self.num_tokens)}
        self.assertEqual(expected_results, results)


class LoadActionDataTest(unittest.TestCase):
    def setUp(self):
        self.net = mock.MagicMock()
        self.parallel_idx = 42

    def test_action_inputs(self):
        self.assertTrue(False)

    def test_collect_inputs_provided_prop_hash(self):
        source_values = {
            'src_0_0': 'a',
            'src_0_1': 'b',
            'src_1_0': 'c',
            'src_1_1': 'd',
        }

        input_connections = {
            '0': {
                'dest_0_0': 'src_0_0',
                'dest_0_1': 'src_0_1',
            },
            '1': {
                'dest_1_0': 'src_1_0',
                'dest_1_1': 'src_1_1',
            },
        }

        net_variables = {}
        for sid, prop_hash in input_connections.iteritems():
            for dest_prop, src_prop in prop_hash.iteritems():
                net_variables[common.output_variable_name(
                    sid, src_prop, self.parallel_idx)] = source_values[src_prop]

        self.net.variable = net_variables.get
        results = load.collect_inputs(self.net,
                input_connections, self.parallel_idx)

        expected_results = {
            'dest_0_0': 'a',
            'dest_0_1': 'b',
            'dest_1_0': 'c',
            'dest_1_1': 'd',
        }
        self.assertEqual(expected_results, results)

    def test_collect_inputs_missing_prop_hash(self):
        input_connections = {
            '0': { },
        }

        net_variables = {
            common.output_variable_name(0, 'src_0_0', self.parallel_idx): 'a'
        }

        self.net.variable = net_variables.get
        operation_output_names = mock.Mock()
        operation_output_names.return_value = ['src_0_0']
        with mock.patch('flow_workflow.io.common.operation_output_names',
                new=operation_output_names):
            results = load.collect_inputs(self.net,
                    input_connections, self.parallel_idx)

        expected_results = {
            'src_0_0': 'a',
        }
        self.assertEqual(expected_results, results)


if __name__ == "__main__":
    unittest.main()
