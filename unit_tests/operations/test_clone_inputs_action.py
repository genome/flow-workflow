from flow.petri_net.actions.base import BasicActionBase
from flow_workflow.operations.clone_inputs_action import CloneInputsAction
from flow_workflow.parallel_id import ParallelIdentifier
from twisted.internet.defer import Deferred

import fakeredis
import mock
import unittest


class CloneInputsActionTest(unittest.TestCase):
    def setUp(self):
        self.connection = fakeredis.FakeRedis()
        self.connection.flushall()

        self.operation_id = 41
        self.input_connections = {u'1': 2, u'3': 4}

        args = {'operation_id': self.operation_id,
                'input_connections': self.input_connections}
        self.key = 'test_action_key'
        self.action = CloneInputsAction.create(
                self.connection, self.key, args=args)

    def tearDown(self):
        self.connection.flushall()

    def test_execute(self):
        net = mock.Mock()
        color_descriptor = mock.Mock()
        active_tokens = mock.Mock()
        parallel_id = ParallelIdentifier()

        expected_tokens = [net.create_token.return_value]
        expected_data = {'workflow_data': {'parallel_id': parallel_id}}

        with mock.patch('flow_workflow.io.extract_workflow_data') as extract:
            with mock.patch('flow_workflow.io.load_inputs') as load:
                with mock.patch('flow_workflow.io.store_outputs') as store:
                    extract.return_value = {'parallel_id': parallel_id}

                    tokens, deferred = self.action.execute(net=net,
                            color_descriptor=color_descriptor,
                            active_tokens=active_tokens,
                            service_interfaces=None)

                    self.assertIsInstance(deferred, Deferred)
                    self.assertItemsEqual(tokens, expected_tokens)

                    extract.assert_called_once_with(net, active_tokens)
                    load.assert_called_once_with(net=net,
                            input_connections=self.input_connections,
                            parallel_id=parallel_id)
                    store.assert_called_once_with(net=net,
                            operation_id=self.operation_id,
                            outputs=load.return_value,
                            parallel_id=parallel_id)
                    net.create_token.assert_called_once_with(
                            color=color_descriptor.color,
                            color_group_idx=color_descriptor.group.idx,
                            data=expected_data)


if __name__ == '__main__':
    unittest.main()
