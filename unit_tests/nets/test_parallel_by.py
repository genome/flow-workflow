import flow_workflow.nets.parallelby as pby

from unittest import TestCase, main
from fakeredis import FakeRedis
import mock

import flow.petri.netbuilder as nb
import flow.petri.safenet as sn
import flow.command_runner.executors.nets as exnets

class NetTest(TestCase):
    def setUp(self):
        self.builder = nb.NetBuilder()
        self.conn = FakeRedis()
        self.time_val = (1363805978, 775085)
        self.conn.time = mock.Mock(return_value=self.time_val)


class TestParallelByNet(NetTest):
    def _create_net(self):
        self.name = "test"
        self.action_type = "command"
        self.action_id = "A::Command"
        self.parent_net_key = "parent_net_key"
        self.parent_operation_id = "12345"
        self.stdout_base = "x.out"
        self.stderr_base = "x.err"

        # Since input_data['a'] has 3 values, we should get 3 parallel
        # commands
        input_names = ["a", "b", "c"]
        self.parallel_by = "a"
        self.input_data = {"a": ["1", "2", "3"], "b": "B", "c": "C"}

        self.success_place = 13
        self.failure_place = 15

        net = self.builder.add_subnet(pby.ParallelByNet,
                name=self.name,
                parallel_by=self.parallel_by,
                input_data=self.input_data,
                parent_net_key=self.parent_net_key,
                parent_operation_id=self.parent_operation_id,
                success_place=self.success_place,
                failure_place=self.failure_place,
                action_type=self.action_type,
                action_id=self.action_id,
                stderr_base=self.stderr_base,
                stdout_base=self.stdout_base)

        return net


    def test_construct(self):
        net = self._create_net()
        self.assertEqual(3, len(net.subnets))

        names = [x.name for x in net.subnets]
        expected = ["test"]*3
        self.assertEqual(expected, names)

    def test_shortcut_updates(self):
        net = self._create_net()
        stored_net = self.builder.store(self.conn)

        token = sn.Token.create(self.conn, data={"pid": 123})

        def test_subnet(idx, subnet):
            historian = mock.Mock()
            services = {"workflow_historian": historian}

            operation_id = idx

            begin_action_idx = subnet.shortcut.t_begin_execute.index
            begin_transition = stored_net.transition(begin_action_idx)
            begin_transition.active_tokens = [token.key]
            begin_action = begin_transition.action
            begin_action.execute(begin_transition.active_tokens.key, stored_net,
                    services)

            print("Checking operation %d" % idx)
            historian.update.assert_called_once_with(
                status=None,
                parent_net_key=self.parent_net_key,
                parent_operation_id=self.parent_operation_id,
                peer_net_key=stored_net.key,
                peer_operation_id=0,
                name=subnet.name,
                start_time=mock.ANY,
                operation_id=operation_id,
                workflow_plan_id=mock.ANY,
                parallel_index=operation_id,
                net_key=stored_net.key,
                dispatch_id='P123')

        for idx, subnet in enumerate(net.subnets):
            test_subnet(idx, subnet)

    def test_execute_updates(self):
        net = self._create_net()
        stored_net = self.builder.store(self.conn)

        token = sn.Token.create(self.conn, data={"pid": 123})

        def test_subnet(idx, subnet):
            historian = mock.Mock()
            services = {"workflow_historian": historian}

            operation_id = idx

            begin_action_idx = subnet.execute.begin_execute.index
            begin_transition = stored_net.transition(begin_action_idx)
            begin_transition.active_tokens = [token.key]
            begin_action = begin_transition.action
            begin_action.execute(begin_transition.active_tokens.key, stored_net,
                    services)

            print("Checking operation %d" % idx)
            historian.update.assert_called_once_with(
                status='running',
                parent_net_key=self.parent_net_key,
                peer_net_key=stored_net.key,
                peer_operation_id=0,
                parent_operation_id=self.parent_operation_id,
                name=subnet.name,
                start_time=mock.ANY,
                operation_id=operation_id,
                workflow_plan_id=mock.ANY,
                parallel_index=operation_id,
                net_key=stored_net.key)

        for idx, subnet in enumerate(net.subnets):
            test_subnet(idx, subnet)



if __name__ == "__main__":
    main()

