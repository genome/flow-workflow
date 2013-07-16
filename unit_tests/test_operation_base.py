from flow_workflow import operation_base

import mock
import unittest


class OperationTest(unittest.TestCase):
    def setUp(self):
        self.child_operation_ids = {
            'foo': 12,
            'bar': 42,
        }
        self.operation_id = 5
        self.name = 'oot'
        self.parent_operation_id = 2
        self.output_properties = ['baz', 'buz']
        self.input_connections = {
            3: {
                'in1': 'out1',
            },
            4: {
                'in2': 'out2',
            },
        }

        self.log_dir = '/exciting/log/dir'

        self.net = mock.Mock()
        self.operation = operation_base.Operation(
                net=self.net,
                child_operation_ids=self.child_operation_ids,
                input_connections=self.input_connections,
                log_dir=self.log_dir,
                name=self.name,
                operation_id=self.operation_id,
                output_properties=self.output_properties,
                parent_operation_id=self.parent_operation_id,
        )

    def test_child_id_from_exists(self):
        self.assertEqual(12, self.operation._child_id_from('foo'))

    def test_child_id_from_missing(self):
        with self.assertRaises(KeyError):
            self.operation._child_id_from('MISSING')


    def test_child_named(self):
        with mock.patch('flow_workflow.factory') as factory:
            child = self.operation.child_named('foo')
            factory.load_operation.assert_called_once_with(net=self.net,
                    operation_id=12)
            self.assertEqual(factory.load_operation.return_value, child)

    def test_parent(self):
        with mock.patch('flow_workflow.factory') as factory:
            parent = self.operation.parent
            factory.load_operation.assert_called_once_with(net=self.net,
                    operation_id=2)
            self.assertEqual(factory.load_operation.return_value, parent)

    def test_log_manager(self):
        self.assertEqual('/exciting/log/dir/oot.5.out',
                self.operation.log_manager.stdout_log_path)

    def test_input_names(self):
        self.assertEqual(['in1', 'in2'], self.operation.input_names)

    def test_load_inputs(self):
        parallel_id = mock.Mock()
        with mock.patch('flow_workflow.operation_base.io') as io:
            inputs = self.operation.load_inputs(parallel_id)
            self.assertItemsEqual(['in1', 'in2'], inputs.keys())
            self.assertEqual(2, io.load_input.call_count)
            io.load_input.assert_any_call(
                    input_connections=self.input_connections,
                    net=self.net, parallel_id=parallel_id, property_name='in1')

    def test_load_outputs(self):
        parallel_id = mock.Mock()
        with mock.patch('flow_workflow.operation_base.io') as io:
            outputs = self.operation.load_outputs(parallel_id)
            self.assertItemsEqual(['buz', 'baz'], outputs.keys())
            self.assertEqual(2, io.load_output.call_count)
            io.load_output.assert_any_call(net=self.net,
                    parallel_id=parallel_id,
                    operation_id=self.operation_id,
                    property_name='buz')

    def test_store_outputs(self):
        parallel_id = mock.Mock()
        outputs = {
            'buz': 'buzdata',
            'baz': 'bazdata',
        }
        with mock.patch('flow_workflow.operation_base.io') as io:
            self.operation.store_outputs(outputs, parallel_id)
            self.assertEqual(2, io.store_output.call_count)
            io.store_output.assert_any_call(
                    net=self.net,
                    property_name='buz',
                    value='buzdata',
                    parallel_id=parallel_id,
                    operation_id=self.operation_id)


if __name__ == "__main__":
    unittest.main()
