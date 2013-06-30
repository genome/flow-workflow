from unittest import TestCase
from flow_workflow.operations.converge.action import ConvergeAction
from flow.petri_net.actions.base import BasicActionBase
import fakeredis
import mock
from mock import patch
from twisted.internet.defer import Deferred


class Tests(TestCase):
    def setUp(self):
        self.connection = fakeredis.FakeRedis()
        self.connection.flushall()

        self.operation_id = 41
        self.input_connections = {'1':2, '3':4}
        self.input_property_order = ['1', '2']
        self.output_properties = ['combined_output']

        args = {'operation_id':self.operation_id,
                'input_connections':self.input_connections,
                'input_property_order':self.input_property_order,
                'output_properties':self.output_properties}
        self.key = 'test_action_key'
        self.action = ConvergeAction.create(
                self.connection, self.key, args=args)

    def tearDown(self):
        self.connection.flushall()

    def test_init(self):
        self.assertIsInstance(self.action, BasicActionBase)

    def test_execute(self):
        net = mock.Mock()
        output_token = mock.Mock()
        net.create_token.return_value = output_token
        color_descriptor = mock.Mock()
        active_tokens = mock.Mock()


        with patch('flow_workflow.io.extract_workflow_data') as extract,\
             patch('flow_workflow.io.load_input') as load,\
             patch('flow_workflow.io.store_outputs') as store,\
             patch('flow_workflow.operations.converge.action.order_outputs') as\
                     order:
            parallel_id = object()
            extract.return_value = {'parallel_id':parallel_id}
            inputs = object()
            load.return_value = inputs
            outputs = object()
            order.return_value = outputs

            tokens, deferred = self.action.execute(net=net,
                    color_descriptor=color_descriptor,
                    active_tokens=active_tokens,
                    service_interfaces=None)

            self.assertIsInstance(deferred, Deferred)
            expected_tokens = [output_token]
            self.assertItemsEqual(tokens, expected_tokens)

            extract.assert_called_once_with(active_tokens)
            load.assert_called_once_with(net=net,
                    input_connections=self.input_connections,
                    parallel_id=parallel_id)
            order.assert_called_once_with(inputs,
                    self.input_property_order,
                    self.output_properties)
            store.assert_called_once_with(net=net,
                    operation_id=self.operation_id,
                    outputs=outputs,
                    parallel_id=parallel_id)
            net.create_token.assert_called_once_with(
                    color=color_descriptor.color,
                    color_group_idx=color_descriptor.group.idx,
                    data={'workflow_data':{'parallel_id':parallel_id}})
