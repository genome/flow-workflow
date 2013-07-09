from flow_workflow.operations.perl_action import actions
from flow_workflow.operations.perl_action import adapter_base
from flow_workflow.operations.perl_action import future_nets
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
    def net(self, **kwargs):
        return adapter_base.PerlActionAdapterBase.net(self, **kwargs)

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

    def test_stderr_log_path(self):
        self.assertEqual('/exciting/log/dir/test_op_name.12345.err',
                self.adapter.stderr_log_path(self.operation_id))

    def test_stdout_log_path(self):
        self.assertEqual('/exciting/log/dir/test_op_name.12345.out',
                self.adapter.stdout_log_path(self.operation_id))

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

    def test_net(self):
        net = self.adapter.net(
                input_connections=mock.Mock(),
                output_properties=mock.Mock(),
                resources=mock.Mock())
        self.assertIsInstance(net, future_nets.PerlActionNet)


class ParallelByPerlActionAdapterBaseTest(unittest.TestCase):
    def setUp(self):
        self.log_dir = '/exciting/log/dir'
        self.operation_id = 12345
        self.parent = mock.Mock()

        self.adapter = FakeAdapter(xml=etree.XML(PARALLEL_BY_XML),
                operation_id=self.operation_id,
                log_dir=self.log_dir, parent=self.parent)

    def test_parallel_by(self):
        self.assertEqual('foo', self.adapter.parallel_by)

    def test_net(self):
        net = self.adapter.net(
                input_connections=mock.Mock(),
                output_properties=mock.Mock(),
                resources=mock.Mock())
        self.assertIsInstance(net, future_nets.ParallelByNet)


if __name__ == '__main__':
    unittest.main()
