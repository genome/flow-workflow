from unittest import TestCase
from flow_workflow.operations.pass_thru_action import PassThruAction
from flow.petri_net.actions.base import BasicActionBase
import fakeredis
import mock
from twisted.internet.defer import Deferred


class Tests(TestCase):
    def setUp(self):
        self.connection = fakeredis.FakeRedis()
        self.connection.flushall()

        self.operation_id = 41
        self.input_connections = {'1':2, '3':4}

        args = {'operation_id':self.operation_id,
                'input_connections':self.input_connections}
        self.key = 'test_action_key'
        self.action = PassThruAction.create(
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


        expected_tokens = [output_token]
        with mock.patch('flow_workflow.io.extract_workflow_data') as extract:
            with mock.patch('flow_workflow.io.load_input') as load:
                with mock.patch('flow_workflow.io.store_outputs') as store:
                    parallel_id = object()
                    extract.return_value = {'parallel_id':parallel_id}
                    inputs = object()
                    load.return_value = inputs

                    tokens, deferred = self.action.execute(net=net,
                            color_descriptor=color_descriptor,
                            active_tokens=active_tokens,
                            service_interfaces=None)

                    self.assertIsInstance(deferred, Deferred)
                    self.assertItemsEqual(tokens, expected_tokens)

                    extract.assert_called_once_with(active_tokens)
                    load.assert_called_once_with(net=net,
                            input_connections=self.input_connections,
                            parallel_id=parallel_id)
                    store.assert_called_once_with(net=net,
                            operation_id=self.operation_id,
                            outputs=inputs,
                            parallel_id=parallel_id)
                    net.create_token.assert_called_once_with(
                            color=color_descriptor.color,
                            color_group_idx=color_descriptor.group.idx,
                            data={'workflow_data':{'parallel_id':parallel_id}})
