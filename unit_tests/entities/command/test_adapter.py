from flow_workflow.entities.command import adapter
from lxml import etree

import mock
import unittest

VALID_XML = '''
<operation name="test_op_name">
    <operationtype typeClass="Workflow::OperationType::Command"
                   commandClass="Test::Command::Class" />
</operation>
'''

class CommandAdapterTest(unittest.TestCase):
    def setUp(self):
        self.operation_id = 12345
        self.adapter = adapter.CommandAdapter(xml=etree.XML(VALID_XML),
                operation_id=self.operation_id)

    def test_action_id(self):
        self.assertEqual('Test::Command::Class', self.adapter.action_id)

    def test_command_class(self):
        self.assertEqual('Test::Command::Class', self.adapter.command_class)


if __name__ == "__main__":
    unittest.main()
