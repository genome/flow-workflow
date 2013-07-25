from flow_workflow.entities.converge import adapters
from flow_workflow.entities.converge import future_nets
from lxml import etree

import mock
import unittest

VALID_XML = '''
<operation name="test_op_name">
    <operationtype typeClass="Workflow::OperationType::Converge">
        <inputproperty>test_input_property</inputproperty>
        <inputproperty>another_test_input_property</inputproperty>
        <outputproperty>all_results</outputproperty>
        <outputproperty>result</outputproperty>
    </operationtype>
</operation>
'''


INVALID_XML = '''
<operation name="op_missing_inputproperties">
    <operationtype typeClass="Workflow::OperationType::Converge">
        <outputproperty>all_results</outputproperty>
        <outputproperty>result</outputproperty>
    </operationtype>
</operation>
'''


class ConvergeAdapterTest(unittest.TestCase):
    def setUp(self):
        self.operation_id = mock.Mock()
        self.parent = mock.Mock()
        self.resources = mock.Mock()

        self.adapter = adapters.ConvergeAdapter(xml=etree.XML(VALID_XML),
                operation_id=self.operation_id, parent=self.parent)

    def test_init(self):
        self.assertIsInstance(self.adapter.future_net(
                resources=self.resources),
            future_nets.ConvergeNet)

    def test_input_property_order(self):
        self.assertEqual(['test_input_property', 'another_test_input_property'],
                self.adapter.input_property_order)

    def test_output_properties(self):
        self.assertItemsEqual(['all_results', 'result'],
                self.adapter.output_properties)


class InvalidConvergeAdapterTest(unittest.TestCase):
    def setUp(self):
        self.operation_id = mock.Mock()
        self.parent = mock.Mock()
        self.resources = mock.Mock()

        self.adapter = adapters.ConvergeAdapter(xml=etree.XML(INVALID_XML),
                operation_id=self.operation_id, parent=self.parent)

    def test_init_raises(self):
        with self.assertRaises(ValueError):
            self.adapter.future_net(resources=self.resources)


if __name__ == "__main__":
    unittest.main()
