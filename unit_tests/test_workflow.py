from flow_workflow import workflow
from lxml import etree

import mock
import unittest


VALID_XML = '''
<operation name="test_model" logDir="/tmp/test/log/dir">
  <operation name="A">
    <operationtype commandClass="NullCommand"
                   typeClass="Workflow::OperationType::Command" />
  </operation>
  <operation name="B">
    <operationtype commandClass="NullCommand"
                   typeClass="Workflow::OperationType::Command" />
  </operation>

  <link fromOperation="input connector" fromProperty="a"
        toOperation="A" toProperty="param" />
  <link fromOperation="input connector" fromProperty="b"
        toOperation="B" toProperty="param" />
  <link fromOperation="A" fromProperty="result"
        toOperation="output connector" toProperty="out_a" />
  <link fromOperation="B" fromProperty="result"
        toOperation="output connector" toProperty="out_b" />

  <operationtype typeClass="Workflow::OperationType::Model">
    <inputproperty>prior_result</inputproperty>
    <outputproperty>result</outputproperty>
  </operationtype>
</operation>
'''


class WorkflowTest(unittest.TestCase):
    def setUp(self):
        self.inputs = {
            'a': 'value of a',
            'b': 'value of b',
        }
        self.resources = {}
        self.workflow = workflow.Workflow(
                xml=etree.XML(VALID_XML),
                inputs=self.inputs,
                resources=self.resources)

    def test_input_connections(self):
        expected_ics = {
            self.workflow.dummy_operation.operation_id:
                {'a': 'a', 'b': 'b'}
        }
        self.assertEqual(expected_ics, self.workflow.input_connections)

    def test_output_properties(self):
        expected_ops = ['out_a', 'out_b']
        self.assertEqual(expected_ops, self.workflow.output_properties)

    def test_store_inputs(self):
        net = mock.Mock()
        with mock.patch('flow_workflow.io.store_outputs') as store:
            self.workflow.store_inputs(net)
            store.assert_called_once_with(net, mock.ANY, self.inputs)


    def test_future_net(self):
        net = self.workflow.future_net
        self.assertIsInstance(net, workflow.WorkflowNet)


if __name__ == '__main__':
    unittest.main()
