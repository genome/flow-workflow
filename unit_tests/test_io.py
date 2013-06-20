from unittest import TestCase, main
from mock import Mock, patch
from flow_workflow import io


class TokenTest(TestCase):
    def setUp(self):
        self.num_tokens = 3
        self.tokens = [Mock() for x in xrange(self.num_tokens)]

        for i, t in enumerate(self.tokens):
            t.data.get.return_value = {
                str(i): 'val_%d' % i,
            }

    def test_extract_data_from_tokens(self):
        results = io.extract_workflow_data(self.tokens)
        expected_results = {str(i): 'val_%d' % i
                for i in xrange(self.num_tokens)}
        self.assertEqual(expected_results, results)


class IOTests(TestCase):
    def setUp(self):
        self.net = Mock()
        self.net.variable = lambda x:x

    def test_load_without_patches(self):
        input_connections = {
                0:{'a':'b'},
                1:{'c':'d'},
        }
        parallel_idx = 99

        return_value = io.load_input(net=self.net,
                input_connections=input_connections,
                name='a',
                parallel_idx=parallel_idx)
        expected_return_value = '_wf_outp_0_b_99'
        self.assertEqual(return_value, expected_return_value)

        return_value = io.load_input(net=self.net,
                input_connections=input_connections,
                name=None,
                parallel_idx=parallel_idx)
        expected_return_value = {
                'a':'_wf_outp_0_b_99',
                'c':'_wf_outp_1_d_99',
        }
        self.assertEqual(return_value, expected_return_value)

    def test_load_input(self):
        input_connections = {
                0:{'a':'b'},
                1:{'c':'d'},
        }
        parallel_idx = object()

        test_load_output = Mock()
        test_load_output.return_value = object()
        with patch('flow_workflow.io.load_output', new=test_load_output):
            # test single
            return_value = io.load_input(net=self.net,
                    input_connections=input_connections,
                    name='a',
                    parallel_idx=parallel_idx)
            self.assertIs(return_value, test_load_output.return_value)
            test_load_output.assert_called_once_with(net=self.net,
                    operation_id=0,
                    property='b',
                    parallel_idx=parallel_idx)

            # should error if input name not in input_connections
            with self.assertRaises(KeyError):
                io.load_input(net=self.net,
                    input_connections=input_connections,
                    name='not_there',
                    parallel_idx=parallel_idx)

            # test multiple
            return_value = io.load_input(net=self.net,
                    input_connections=input_connections,
                    name=None,
                    parallel_idx=parallel_idx)
            expected_return_value = {
                    'a':test_load_output.return_value,
                    'c':test_load_output.return_value,
                    }
            self.assertEqual(return_value, expected_return_value)


    def test_load_output(self):
        operation_id = object()
        property = object()
        parallel_idx = object()

        test_variable_name = Mock()
        test_variable_name.return_value = object()
        with patch('flow_workflow.io._output_variable_name',
                new=test_variable_name):
            return_value = io.load_output(net=self.net,
                    operation_id=operation_id,
                    property=property,
                    parallel_idx=parallel_idx)
            self.assertIs(return_value, test_variable_name.return_value)
            test_variable_name.assert_called_once_with(
                    operation_id=operation_id, property=property,
                    parallel_idx=parallel_idx)

    def test_store_output(self):
        self.net.set_variable = Mock()
        self.net.set_variable.return_value = object()

        operation_id = 44
        property = 'test_output_name'
        value = object()
        parallel_idx = 99

        return_value = io.store_output(net=self.net,
                operation_id=operation_id,
                property=property,
                value=value,
                parallel_idx=parallel_idx)
        self.assertIs(return_value, None)

        self.net.set_variable.assert_called_once_with(
            '_wf_outp_44_test_output_name_99', value)

    def test_store_outputs(self):
        operation_id = object()
        outputs = {0:1, 2:3}
        parallel_idx = object()

        test_store_output = Mock()
        with patch('flow_workflow.io.store_output', new=test_store_output):
            return_value = io.store_outputs(net=self.net,
                    operation_id=operation_id,
                    outputs=None,
                    parallel_idx=parallel_idx)
            self.assertIs(return_value, None)
            self.assertEqual(test_store_output.call_count, 0)

            return_value = io.store_outputs(net=self.net,
                    operation_id=operation_id,
                    outputs=outputs,
                    parallel_idx=parallel_idx)
            self.assertIs(return_value, None)

            test_store_output.assert_any_call(net=self.net,
                    operation_id=operation_id,
                    property=0,
                    value=1,
                    parallel_idx=parallel_idx)

            test_store_output.assert_any_call(net=self.net,
                    operation_id=operation_id,
                    property=2,
                    value=3,
                    parallel_idx=parallel_idx)


    def test_private_output_variable_name(self):
        operation_id = 44
        property = 'test_property'
        parallel_idx = 99

        return_value = io._output_variable_name(operation_id=operation_id,
                property=property,
                parallel_idx=parallel_idx)
        self.assertEqual(return_value, '_wf_outp_44_test_property_99')

        return_value = io._output_variable_name(operation_id=operation_id,
                property=property)
        self.assertEqual(return_value, '_wf_outp_44_test_property')


if __name__ == "__main__":
    main()
