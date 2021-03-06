from flow_workflow import operation_base
from flow_workflow.log_manager import LogManager

import mock
import unittest


class DirectStorageOperationTest(unittest.TestCase):
    def setUp(self):
        self.net_key = 'mykey'
        self.children = {
            'foo': (self.net_key, 12),
            'bar': (self.net_key, 42),
        }
        self.operation_id = 5
        self.name = 'oot'
        self.parent_operation_id = 2
        self.parent_net_key = 'parent_net_key'
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

        self.net = mock.MagicMock()
        self.operation = operation_base.DirectStorageOperation(
                net=self.net,
                children=self.children,
                input_connections=self.input_connections,
                log_dir=self.log_dir,
                name=self.name,
                operation_id=self.operation_id,
                output_properties=self.output_properties,
                parent_operation_id=self.parent_operation_id,
                parent_net_key=self.parent_net_key,
        )

    def test_child_net_key_and_id_from_exists(self):
        self.assertEqual((self.net_key, 12),
                self.operation._child_net_key_and_id_from('foo'))

    def test_child_net_key_and_id_from_missing(self):
        with self.assertRaises(KeyError):
            self.operation._child_net_key_and_id_from('MISSING')


    def test_child_named(self):
        with mock.patch('flow_workflow.operation_base.load_operation') as load:
            child = self.operation.child_named('foo')
            load.assert_called_once_with(net=mock.ANY,
                    operation_id=12)
            self.assertEqual(load.return_value, child)


    def test_iter_children(self):
        load = mock.Mock()
        self.operation._load_operation = load

        children = list(self.operation.iter_children())
        self.assertEqual([load.return_value, load.return_value], children)

        self.assertEqual(2, load.call_count)
        load.assert_any_call(self.net_key, 12)
        load.assert_any_call(self.net_key, 42)

    def test_null_parent(self):
        self.operation.parent_operation_id = None
        self.assertIsInstance(self.operation.parent,
                operation_base.NullOperation)

    def test_parent(self):
        with mock.patch('flow_workflow.operation_base.load_operation') as load:
            parent = self.operation.parent
            load.assert_called_once_with(net=mock.ANY, operation_id=2)
            self.assertEqual(load.return_value, parent)

    def test_log_manager(self):
        self.assertIsInstance(self.operation.log_manager, LogManager)
        self.assertEqual(self.log_dir, self.operation.log_manager.log_dir)

    def test_input_names(self):
        self.assertEqual(['in1', 'in2'], self.operation.input_names)

    def test_load_inputs(self):
        parallel_id = mock.Mock()
        load_input = mock.Mock()
        self.operation.load_input = load_input

        inputs = self.operation.load_inputs(parallel_id)
        self.assertItemsEqual(['in1', 'in2'], inputs.keys())
        self.assertEqual(2, load_input.call_count)
        load_input.assert_any_call(name='in1', parallel_id=parallel_id)

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

    def test_determine_input_source_success(self):
        self.assertEqual((3, 'out1'),  # Fakeredis makes these unicode..
                self.operation._determine_input_source('in1'))

    def test_determine_input_source_error(self):
        with self.assertRaises(KeyError):
            self.operation._determine_input_source('nonsense_property')


if __name__ == "__main__":
    unittest.main()
