from flow_workflow.entities.block import adapter
from flow_workflow.clone_inputs_future_net import CloneInputsNet
from lxml import etree

import mock
import unittest

VALID_XML = '''
<operation name="test_op_name">
    <operationtype typeClass="Workflow::OperationType::Block" />
</operation>
'''

class BlockAdapterTest(unittest.TestCase):
    def setUp(self):
        self.operation_id = mock.Mock()
        self.parent = mock.Mock()
        self.adapter = adapter.BlockAdapter(xml=etree.XML(VALID_XML),
                operation_id=self.operation_id,
                parent=self.parent)

    def test_future_net(self):
        input_connections = mock.Mock()
        resources = mock.Mock()

        self.assertIsInstance(self.adapter.future_net(
                input_connections=input_connections,
                output_properties=mock.Mock(),
                resources=resources),
            CloneInputsNet)


if __name__ == "__main__":
    unittest.main()
