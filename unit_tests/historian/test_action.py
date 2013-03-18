from flow_workflow.historian.action import WorkflowHistorianUpdateAction

import flow.petri.safenet as sn

import fakeredis
import unittest
import mock


class TestAction(unittest.TestCase):
    def setUp(self):
        self.user_name = 'flow_user'

        self.conn = fakeredis.FakeRedis()
        self.conn.time = mock.Mock(return_value=(1363285207, 324852))

        self.historian = mock.Mock()
        self.services = {'workflow_historian': self.historian}

        self.token = sn.Token.create(self.conn)
        self.active_tokens_key = 'tokens'
        self.conn.lpush(self.active_tokens_key, self.token.key)

        self.net_constants = {
                'user_name': self.user_name,
                'workflow_parent_net_key': 'parent_net_key',
                'workflow_parent_operation_id': 123,
                'workflow_plan_id': 321
                }

        self.net = mock.Mock()
        self.net.key = 'netkey!'
        self.net.constant.side_effect = self.net_constants.get

    def test_null_operation_id(self):
        cinfo = [{'status': 'new', 'parent_operation_id': 10}]
        args = {'children_info': cinfo}

        action = WorkflowHistorianUpdateAction.create(self.conn, args=args)

        self.assertRaises(RuntimeError, action.execute, self.active_tokens_key,
                self.net, self.services)

    def test_single(self):
        cinfo = [{'id': 42, 'status': 'new', 'parent_operation_id': 12}]
        args = {'children_info': cinfo}

        action = WorkflowHistorianUpdateAction.create(self.conn, args=args)

        action.execute(self.active_tokens_key, self.net, self.services)

        self.historian.update.assert_called_once_with(
                status='new',
                parent_net_key=self.net.key,
                parent_operation_id=12,
                workflow_plan_id=321,
                net_key=self.net.key,
                operation_id=42)

    def test_nested(self):
        # When parent_operation_id is omitted, from 'children_info', but
        # workflow_parent_net_key and workflow_parent_operation_id exist as
        # net constants, the latter are used as the parent info

        cinfo = [{'id': 42, 'status': 'new'}]
        args = {'children_info': cinfo}

        action = WorkflowHistorianUpdateAction.create(self.conn, args=args)

        action.execute(self.active_tokens_key, self.net, self.services)

        self.historian.update.assert_called_once_with(
                status='new',
                parent_net_key='parent_net_key',
                parent_operation_id=123,
                workflow_plan_id=321,
                net_key=self.net.key,
                operation_id=42,
                is_subflow=True)

    def test_no_parent(self):
        cinfo = [{'id': 42, 'status': 'new'}]
        args = {'children_info': cinfo}

        action = WorkflowHistorianUpdateAction.create(self.conn, args=args)

        net = mock.Mock()
        net.key = 'netkey!'
        net.constant.side_effect = {'workflow_plan_id': 321}.get

        action.execute(self.active_tokens_key, net, self.services)

        self.historian.update.assert_called_once_with(
                status='new',
                workflow_plan_id=321,
                net_key=net.key,
                operation_id=42)

    def test_token_data_map_and_timestamps(self):
        cinfo = [{'id': 42, 'status': 'new', 'parent_operation_id': 10}]
        args = {'children_info': cinfo,
                'token_data_map': {'pid': 'dispatch_id'},
                'timestamps': ['start_time']}

        action = WorkflowHistorianUpdateAction.create(self.conn, args=args)

        self.token.data = {'pid': '555'}

        action.execute(self.active_tokens_key, self.net, self.services)

        self.historian.update.assert_called_once_with(
                status='new',
                parent_net_key=self.net.key,
                parent_operation_id=10,
                workflow_plan_id=321,
                net_key=self.net.key,
                operation_id=42,
                dispatch_id='555',
                start_time=action._timestamp())

    def test_shortcut_pid(self):
        cinfo = [{'id': 42, 'status': 'new', 'parent_operation_id': 10}]
        args = {'children_info': cinfo,
                'token_data_map': {'pid': 'dispatch_id'},
                'timestamps': ['start_time'],
                'shortcut': True}

        action = WorkflowHistorianUpdateAction.create(self.conn, args=args)

        self.token.data = {'pid': '555'}

        action.execute(self.active_tokens_key, self.net, self.services)

        self.historian.update.assert_called_once_with(
                status='new',
                parent_net_key=self.net.key,
                parent_operation_id=10,
                workflow_plan_id=321,
                net_key=self.net.key,
                operation_id=42,
                dispatch_id='P555',
                start_time=action._timestamp())

    def test_net_constants(self):
        cinfo = [{'id': 42, 'status': 'new', 'parent_operation_id': 10}]
        args = {'children_info': cinfo,
                'net_constants_map': {'user_name': 'user_name'}}

        action = WorkflowHistorianUpdateAction.create(self.conn, args=args)

        action.execute(self.active_tokens_key, self.net, self.services)

        self.historian.update.assert_called_once_with(
                status='new',
                parent_net_key=self.net.key,
                parent_operation_id=10,
                workflow_plan_id=321,
                net_key=self.net.key,
                operation_id=42,
                user_name=self.user_name)



if __name__ == '__main__':
    unittest.main()
