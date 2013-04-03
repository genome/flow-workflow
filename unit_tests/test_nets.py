import flow_workflow.nets as wfnets
from unittest import TestCase, main
from mock import Mock, MagicMock

import flow.petri.netbuilder as nb
import flow.command_runner.executors.nets as exnets
from fakeredis import FakeRedis

class TestableInputs(wfnets.InputsMixin):
    def __init__(self, args):
        self.args = args


class TestInputs(TestCase):
    def setUp(self):
        self.variables = {
                wfnets.output_variable_name(0, "x"): [1, 2, 3, 4],
                wfnets.output_variable_name(0, "y"): "y",
                wfnets.output_variable_name(1, "z"): "z",
                wfnets.op_outputs_variable_name(0): ["x", "y"],
                wfnets.op_outputs_variable_name(1): ["z"],
                }

        self.data_arcs = {
                2: {
                    0: {"X": "x", "Y": "y"},
                    1: {"Z": "z"},
                }}

        self.net = Mock()
        self.net.variable = Mock(wraps=self.variables.get)

    def test_normal(self):
        expected_inputs = {"X": [1, 2, 3, 4], "Y": "y", "Z": "z"}

        input_conns = self.data_arcs[2]

        test_obj = TestableInputs(args={"input_connections": input_conns})
        inputs = test_obj.input_data(active_tokens_key=None, net=self.net)
        self.assertEqual(expected_inputs, inputs)

    def test_parallel_by(self):
        input_conns = self.data_arcs[2]

        args = {"input_connections": input_conns, "parallel_by": "X"}
        test_obj = TestableInputs(args=args)

        test_obj.args["parallel_by_idx"] = 0
        inputs = test_obj.input_data(active_tokens_key=None, net=self.net)
        expected_inputs = {"X": 1, "Y": "y", "Z": "z"}
        self.assertEqual(expected_inputs, inputs)

        test_obj.args["parallel_by_idx"] = 1
        inputs = test_obj.input_data(active_tokens_key=None, net=self.net)
        expected_inputs = {"X": 2, "Y": "y", "Z": "z"}
        self.assertEqual(expected_inputs, inputs)

        test_obj.args["parallel_by_idx"] = 2
        inputs = test_obj.input_data(active_tokens_key=None, net=self.net)
        expected_inputs = {"X": 3, "Y": "y", "Z": "z"}
        self.assertEqual(expected_inputs, inputs)

        test_obj.args["parallel_by_idx"] = 3
        inputs = test_obj.input_data(active_tokens_key=None, net=self.net)
        expected_inputs = {"X": 4, "Y": "y", "Z": "z"}
        self.assertEqual(expected_inputs, inputs)

    def test_null_connection_means_all(self):
        args = {"input_connections": {0: {}}}

        test_obj = TestableInputs(args=args)
        inputs = test_obj.input_data(active_tokens_key=None, net=self.net)
        expected_inputs = {"x": [1, 2, 3, 4], "y": "y"}
        self.assertEqual(expected_inputs, inputs)


class TestGenomeAction(TestCase):
    def test_service_names(self):
        self.assertEqual("fork", wfnets.GenomeShortcutAction.service_name)
        self.assertEqual("lsf", wfnets.GenomeExecuteAction.service_name)


class NetTest(TestCase):
    def setUp(self):
        self.builder = nb.NetBuilder()


class TestGenomePerlActionNet(NetTest):
    def test_genome_shortcut_action(self):
        input_conns = {123: {"x": "y"}}

        net = wfnets.GenomePerlActionNet(self.builder,
                name="test",
                operation_id=6,
                parent_operation_id=None,
                input_connections=input_conns,
                action_type="command",
                action_id="ClassX")

        self.assertIsInstance(net.start_transition, nb.Transition)
        self.assertIsInstance(net.success_transition, nb.Transition)
        self.assertIsInstance(net.failure_transition, nb.Transition)
        self.assertIsInstance(net.shortcut, exnets.LocalCommandNet)
        self.assertIsInstance(net.execute, exnets.LSFCommandNet)


class TestBuildParallelByAction(NetTest):
    def test_construct(self):
        inputs = {"file": ["f%d" % x for x in xrange(20)]}
        obj = Mock(wfnets.BuildParallelByAction)
        obj.name = "Parallel By"
        obj.connection = FakeRedis()

        obj.args = {"action_type": "command", "action_id": "ClassX",
                "parallel_by": "file", "success_place": 1, "failure_place": 2,
                "parent_operation_id": None, "peer_operation_id": 12,
                "stdout_base": "out", "stderr_base": "err"}
        obj.input_data = Mock(return_value=inputs)

        net = MagicMock()
        net.key = "netkey!"

        orchestrator = Mock()

        service_interfaces = {"orchestrator": orchestrator}
        wfnets.BuildParallelByAction.execute(obj,
                active_tokens_key=None, net=net, service_interfaces=service_interfaces)


if __name__ == "__main__":
    main()
