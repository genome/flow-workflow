import flow_workflow.nets.parallelby as pby
from unittest import TestCase, main

import flow.petri.netbuilder as nb
import flow.command_runner.executors.nets as exnets

class NetTest(TestCase):
    def setUp(self):
        self.builder = nb.NetBuilder()

class TestParallelByNet(NetTest):
    def test_construct(self):
        name = "test"
        action_type = "command"
        action_id = "A::Command"
        parent_net_key = "parent_net_key"
        parent_operation_id = "123"
        peer_operation_id = "42"
        stdout_base = "x.out"
        stderr_base = "x.err"

        # Since input_data['a'] has 3 values, we should get 3 parallel
        # commands
        input_names = ["a", "b", "c"]
        parallel_by = "a"
        input_data = {"a": ["1", "2", "3"], "b": "B", "c": "C"}

        success_place = 13
        failure_place = 15

        net = self.builder.add_subnet(pby.ParallelByNet, name, parallel_by,
                input_data, parent_net_key, success_place, failure_place,
                action_type, action_id, parent_operation_id, peer_operation_id,
                stderr_base, stdout_base)

        self.assertEqual(3, len(net.subnets))

        self.builder.graph().draw("x.ps", prog="dot")


if __name__ == "__main__":
    main()

