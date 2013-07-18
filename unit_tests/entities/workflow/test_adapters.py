from flow_workflow.future_operation import NullFutureOperation
from flow_workflow.parallel_id import ParallelIdentifier
from lxml import etree

import mock
import unittest
from flow_workflow.entities.workflow.adapter import WorkflowAdapter
from flow_workflow.entities.workflow.future_nets import WorkflowNet


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
        self.workflow = WorkflowAdapter(
                xml=etree.XML(VALID_XML),
                inputs=self.inputs)

    def test_input_connections(self):
        expected_ics = {
            self.workflow.inputs_storage_adapter.operation_id:
                {'a': 'a', 'b': 'b'}
        }
        self.assertEqual(expected_ics, self.workflow.input_connections)

    def test_output_properties(self):
        expected_ops = ['out_a', 'out_b']
        self.assertEqual(expected_ops, self.workflow.output_properties)

    def test_future_net(self):
        net = self.workflow.future_net(self.resources)
        self.assertIsInstance(net, WorkflowNet)

    def test_future_operations(self):
        future_ops = self.workflow.future_operations(NullFutureOperation(),
                input_connections=mock.Mock(),
                output_properties=mock.Mock())
        self.assertEqual(6, len(future_ops))



if __name__ == '__main__':
    unittest.main()
