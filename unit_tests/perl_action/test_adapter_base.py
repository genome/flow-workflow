from flow_workflow.perl_action import actions
from flow_workflow.perl_action import adapter_base
from flow_workflow.perl_action import future_nets
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


class FakeAdapter(adapter_base.PerlActionAdapterBase):
    operation_class = 'fake'

    def single_future_net(self, **kwargs):
        return adapter_base.PerlActionAdapterBase.single_future_net(self,
                **kwargs)

    @property
    def action_type(self):
        return 'foo'

    @property
    def action_id(self):
        return 12345


class NormalPerlActionAdapterBaseTest(unittest.TestCase):
    def setUp(self):
        self.log_dir = '/exciting/log/dir'
        self.operation_id = 12345
        self.parent = mock.Mock()

        self.adapter = FakeAdapter(xml=etree.XML(NORMAL_XML),
                operation_id=self.operation_id,
                log_dir=self.log_dir, parent=self.parent)

    def test_parallel_by(self):
        self.assertEqual(None, self.adapter.parallel_by)

    def test_shortcut_action_class(self):
        self.assertEqual(actions.ForkAction,
                self.adapter.shortcut_action_class)

    def test_execute_action_class_remote(self):
        self.assertEqual(actions.LSFAction,
                self.adapter.execute_action_class)

    def test_execute_action_class_local(self):
        self.adapter.local_workflow = True
        self.assertEqual(actions.ForkAction,
                self.adapter.execute_action_class)

    def test_future_net(self):
        net = self.adapter.future_net(
                resources=mock.Mock())
        self.assertIsInstance(net, future_nets.PerlActionNet)


if __name__ == '__main__':
    unittest.main()
