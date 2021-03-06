from test_helpers import fakeredistest
from flow.petri_net.actions.base import BasicActionBase
from flow_workflow.entities.converge.actions import ConvergeAction, order_outputs
from twisted.internet.defer import Deferred

import fakeredis
import mock
import unittest


class OrderOutputsTest(unittest.TestCase):
    def test_order_outputs(self):
        inputs = {
            'foo': 7,
            'baz': 14,
            'bar': 42,
        }
        input_property_order = ['foo', 'bar', 'baz']
        output_properties = ['ofuz', 'obuz', 'omuz']
        expected_result = {
            'ofuz': [7, 42, 14],
            'result': 'BQcKATE=\n',
        }

        self.assertEqual(expected_result, order_outputs(inputs,
            input_property_order, output_properties))


class ConvergeActionTest(fakeredistest.FakeRedisTest):
    def setUp(self):
        fakeredistest.FakeRedisTest.setUp(self)

        self.operation_id = 41
        self.input_connections = {
            '1': {'foo': 'srcfoo'},
            '2': {'bar': 'srcbar'},
        }
        self.input_property_order = ['foo', 'bar']
        self.output_properties = ['oprop', 'unused_oprop']

        args = {
            'input_connections': self.input_connections,
            'input_property_order': self.input_property_order,
            'operation_id': self.operation_id,
            'output_properties': self.output_properties,
        }
        self.key = 'test_action_key'
        self.action = ConvergeAction.create(
                self.conn, self.key, args=args)

        self.net = mock.MagicMock()
        self.parallel_id = [(41, 7)]

    def test_execute(self):
        active_tokens = mock.MagicMock()
        service_interfaces = mock.MagicMock()
        color_descriptor = mock.MagicMock()

        self.action.converge_inputs = mock.Mock()

        workflow_data = {'parallel_id': self.parallel_id}
        with mock.patch('flow_workflow.io.extract_workflow_data') as wfdata:
            wfdata.return_value = workflow_data
            with mock.patch('flow_workflow.io.store_outputs') as store:
                self.action.execute(net=self.net,
                        color_descriptor=color_descriptor,
                        active_tokens=active_tokens,
                        service_interfaces=service_interfaces)

                store.assert_called_once_with(net=self.net,
                        operation_id=self.operation_id,
                        outputs=self.action.converge_inputs.return_value,
                        parallel_id=self.parallel_id)

        self.net.create_token.asser_called_once_with(
                color=color_descriptor.color,
                color_group_idx=color_descriptor.group.idx,
                data={'workflow_data': workflow_data})

    def test_converge_inputs(self):
        inputs = {
            'foo': mock.Mock(),
            'bar': mock.Mock(),
        }

        operation = mock.Mock()
        operation.load_inputs.return_value = inputs

        with mock.patch('flow_workflow.factory.load_operation') as load_op:
            load_op.return_value = operation

            expected_outputs = [inputs[x] for x in self.input_property_order]
            self.assertEqual({u'oprop': expected_outputs, 'result': 'BQcKATE=\n'},
                    self.action.converge_inputs(net=self.net,
                        parallel_id=self.parallel_id))

            load_op.assert_called_once_with(self.net,
                    self.operation_id)


if __name__ == "__main__":
    unittest.main()
