from flow_workflow.io import common
from flow_workflow.io import store

import mock
import unittest


class StoreVariableTest(unittest.TestCase):
    def setUp(self):
        self.net = mock.Mock()
        self.parallel_idx = 3
        self.operation_id = 12345

        self.outputs_variable_name = common.op_outputs_variable_name(
                self.operation_id, self.parallel_idx)


    def test_store_nothing(self):
        outputs = {}
        store.store_outputs(outputs, self.net,
                self.operation_id, self.parallel_idx)

        self.assertEqual(0, len(self.net.mock_calls))

    def test_store_something(self):
        outputs = {'foo': ['a', 'b', 'c'], 'bar': 7}
        store.store_outputs(outputs, self.net,
                self.operation_id, self.parallel_idx)

        for name, value in outputs.iteritems():
            self.net.set_variable.assert_any_call(
                    common.output_variable_name(self.operation_id,
                    name, self.parallel_idx),
                    value)

        self.net.set_variable.assert_any_call(
                self.outputs_variable_name, outputs.keys())
        self.assertEqual(3, len(self.net.set_variable.mock_calls))


if __name__ == "__main__":
    unittest.main()
