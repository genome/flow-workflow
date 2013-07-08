from flow_workflow.operations import factory
from lxml import etree

import mock
import unittest


VALID_XML = '''
<operation name="test_op_name">
    <operationtype commandClass="NullCommand"
                   typeClass="Workflow::OperationType::Command" />
</operation>
'''


class OperationTypeTest(unittest.TestCase):
    def test_get_operation_type_valid(self):
        xml = etree.XML(VALID_XML)
        self.assertEqual('Workflow::OperationType::Command',
                factory.get_operation_type(xml))


if __name__ == '__main__':
    unittest.main()
