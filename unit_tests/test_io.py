from flow.petri_net.net import Net
from flow_workflow import io
from test_helpers.fakeredistest import FakeRedisTest

import mock
import unittest


class ExtractWorkflowDataTest(unittest.TestCase):
    def setUp(self):
        self.num_tokens = 3
        self.net = mock.MagicMock()
        self.token_keys = [str(x) for x in xrange(self.num_tokens)]
        self.tokens = [mock.Mock() for x in xrange(self.num_tokens)]
        self.net.token.side_effect = self.tokens

        for i, t in enumerate(self.tokens):
            t.data.get.return_value = {
                str(i): 'val_%d' % i,
            }

    def test_extract_data_from_tokens(self):
        results = io.extract_workflow_data(self.net, self.token_keys)
        expected_results = {str(i): 'val_%d' % i
                for i in xrange(self.num_tokens)}
        self.assertEqual(expected_results, results)


class VariableNameTest(unittest.TestCase):
    def test_private_output_variable_name(self):
        operation_id = 44
        property_name = 'test_property'
        parallel_id = [[3, 4], [1, 2], [5, 6]]

        return_value = io._output_variable_name(operation_id=operation_id,
                property_name=property_name,
                parallel_id=parallel_id)
        self.assertEqual(return_value, '_wf_outp_44_test_property|3:4|1:2|5:6')

        return_value = io._output_variable_name(operation_id=operation_id,
                property_name=property_name)
        self.assertEqual(return_value, '_wf_outp_44_test_property')


class StoreLoadTest(FakeRedisTest):
    def setUp(self):
        FakeRedisTest.setUp(self)

        self.net = Net.create(self.conn, key='netkey')

        self.output_operation_id = 4
        self.input_property_name = 'foo'
        self.output_property_name = 'bar'
        self.input_connections = {
            4: {self.input_property_name: self.output_property_name}
        }
        self.value = 'awesome data'

    def test_store_output(self):
        parallel_id = []

        io.store_output(net=self.net, operation_id=self.output_operation_id,
                property_name=self.output_property_name, value=self.value,
                parallel_id=parallel_id)

        only_value = self.net.variables.value.values()[0]
        self.assertEqual(self.value, only_value)


    def store_output_then_load_output(self, parallel_id):
        io.store_output(net=self.net, operation_id=self.output_operation_id,
                property_name=self.output_property_name, value=self.value,
                parallel_id=parallel_id)

        self.assertEqual(self.value,
                io.load_output(net=self.net,
                    operation_id=self.output_operation_id,
                    property_name=self.output_property_name,
                    parallel_id=parallel_id))

    def test_store_output_then_load_output_no_parallel_id(self):
        self.store_output_then_load_output([])

    def test_store_output_then_load_output_with_parallel_id(self):
        self.store_output_then_load_output([[24, 17]])


    def store_output_then_load_input(self, store_parallel_id, load_parllel_id):
        io.store_output(net=self.net, operation_id=self.output_operation_id,
                property_name=self.output_property_name, value=self.value,
                parallel_id=store_parallel_id)

        self.assertEqual(self.value,
                io.load_input(net=self.net,
                    input_connections=self.input_connections,
                    property_name=self.input_property_name,
                    parallel_id=load_parllel_id))

    def test_store_output_load_input_no_parallel_id(self):
        self.store_output_then_load_input([], [])

    def test_store_output_load_input_same_parallel_id(self):
        parallel_id = [[4, 7]]
        self.store_output_then_load_input(parallel_id, parallel_id)

    def test_store_output_load_input_different_parallel_id(self):
        store_parallel_id = [[4, 7]]
        load_parallel_id = store_parallel_id + [[12, 17]]
        self.store_output_then_load_input(store_parallel_id, load_parallel_id)

    def test_store_output_empty_id_load_input_with_id(self):
        store_parallel_id = []
        load_parallel_id = [[12, 17]]
        self.store_output_then_load_input(store_parallel_id, load_parallel_id)


    def store_outputs_then_load_inputs(self, store_parallel_id,
            load_parallel_id):
        outputs = {
            'bar1': 'value A',
            'bar2': 'value B'
        }
        input_connections = {
            self.output_operation_id: {
                'foo1': 'bar1',
                'foo2': 'bar2',
            }
        }

        io.store_outputs(net=self.net, operation_id=self.output_operation_id,
                outputs=outputs, parallel_id=store_parallel_id)

        expected_inputs = {
            'foo1': 'value A',
            'foo2': 'value B',
        }
        self.assertEqual(expected_inputs, io.load_inputs(net=self.net,
            input_connections=input_connections, parallel_id=load_parallel_id))

    def test_store_outptus_load_inputs_no_parallel_id(self):
        self.store_outputs_then_load_inputs([], [])

    def test_store_outptus_load_inputs_mismatched_parallel_id(self):
        store_parallel_id = []
        load_parallel_id = [[7, 24]]
        self.store_outputs_then_load_inputs(store_parallel_id, load_parallel_id)


if __name__ == "__main__":
    unittest.main()
