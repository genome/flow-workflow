from flow_workflow.future_operation import FutureOperation

import mock
import unittest


class FutureOperationTest(unittest.TestCase):
    def setUp(self):
        self.operation_class = mock.Mock()
        self.name = 'my op name'
        self.operation_id = 12345
        self.input_connections = mock.Mock()
        self.output_properties = mock.Mock()
        self.log_dir = mock.Mock()
        self.parent = mock.Mock()

        self.future_operation = FutureOperation(
                name=self.name,
                operation_class=self.operation_class,
                operation_id=self.operation_id,
                input_connections=self.input_connections,
                output_properties=self.output_properties,
                parent=self.parent,
                log_dir=self.log_dir)



    def test_operation(self):
        net = mock.Mock()
        self.assertEqual(self.operation_class.return_value,
                self.future_operation.operation(net))
        self.operation_class.assert_called_once_with(
                net=net,
                name=self.name,
                operation_id=self.operation_id,
                input_connections=self.input_connections,
                output_properties=self.output_properties,
                parent_operation_id=self.parent.operation_id,
                log_dir=self.log_dir,
                child_operation_ids=mock.ANY)


    def test_children_ids_no_children(self):
        self.assertEqual({}, self.future_operation.children_ids)

    def test_children_ids_one_child(self):
        child = FutureOperation(
                name='my child name',
                operation_id=12346,
                parent=self.future_operation,
                operation_class=self.operation_class,
                input_connections=self.input_connections,
                output_properties=self.output_properties,
                log_dir=self.log_dir)

        self.assertEqual({'my child name': 12346},
                self.future_operation.children_ids)

    def test_init_adds_parent(self):
        self.parent._add_child.assert_called_once_with(self.future_operation)


if __name__ == "__main__":
    unittest.main()
