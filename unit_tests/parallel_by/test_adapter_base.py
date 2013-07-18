from flow_workflow.future_nets import WorkflowNetBase
from flow_workflow.parallel_by import actions
from flow_workflow.parallel_by import adapter_base
from flow_workflow.parallel_by import future_nets
from lxml import etree

import mock
import unittest


PARALLEL_BY_XML = '''
<operation name="test_op_name" parallelBy="foo">
    <operationtype commandClass="NullCommand"
                   typeClass="Workflow::OperationType::Command" />
</operation>
'''


NORMAL_XML = '''
<operation name="test_op_name">
    <operationtype commandClass="NullCommand"
                   typeClass="Workflow::OperationType::Command" />
</operation>
'''


class FakeAdapter(adapter_base.ParallelXMLAdapterBase):
    operation_class = 'fake'

    def single_future_net(self, **kwargs):
        return WorkflowNetBase(name=self.name, operation_id=54321)

    @property
    def action_type(self):
        return 'foo'

    @property
    def action_id(self):
        return 12345


class ParallelByPerlActionAdapterBaseTest(unittest.TestCase):
    def setUp(self):
        self.operation_id = 12345
        self.parent = mock.Mock()
        self.future_net = mock.Mock()

        self.adapter = FakeAdapter(xml=etree.XML(PARALLEL_BY_XML),
                operation_id=self.operation_id,
                parent=self.parent)

    def test_parallel_by(self):
        self.assertEqual('foo', self.adapter.parallel_by)

    def test_future_net(self):
        net = self.adapter.future_net(
                input_connections=mock.Mock(),
                output_properties=mock.Mock(),
                resources=mock.Mock())
        self.assertIsInstance(net, future_nets.ParallelByNet)


if __name__ == '__main__':
    unittest.main()
