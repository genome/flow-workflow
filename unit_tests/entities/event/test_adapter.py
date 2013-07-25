from flow_workflow.entities.event import adapters
from lxml import etree

import mock
import unittest

VALID_XML = '''
<operation name="test_op_name">
    <operationtype typeClass="Workflow::OperationType::Event"
                   eventId="6789" />
</operation>
'''

class EventAdapterTest(unittest.TestCase):
    def setUp(self):
        self.operation_id = 12345
        self.adapter = adapters.EventAdapter(xml=etree.XML(VALID_XML),
                operation_id=self.operation_id)

    def test_action_id(self):
        self.assertEqual('6789', self.adapter.action_id)

    def test_event_id(self):
        self.assertEqual('6789', self.adapter.event_id)


if __name__ == "__main__":
    unittest.main()
