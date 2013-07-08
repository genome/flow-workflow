from mock import Mock, PropertyMock
from unittest import TestCase, main
import flow_workflow.nets as wfnets

import flow.petri.netbuilder as nb
import flow.shell_command.executors.nets as exnets
import flow.redisom as rom
from fakeredis import FakeRedis

class TestableInputs(wfnets.InputsMixin):
    def __init__(self, args, tokens):
        self.args = args
        self._tokens = tokens

    def tokens(self, active_tokens_key):
        return self._tokens


class TestInputs(TestCase):
    def setUp(self):
        self.variables = {
                # Normal / color=0 values
                wfnets.output_variable_name(0, "x"): [1, 2, 3, 4],
                wfnets.output_variable_name(0, "y"): "y",
                wfnets.output_variable_name(1, "z"): "z",
                wfnets.op_outputs_variable_name(0): ["x", "y"],
                wfnets.op_outputs_variable_name(1): ["z"],

                # Color (aka parallel_idx)=1 values
                wfnets.output_variable_name(0, "x", 1): [5, 6, 7, 8],
                wfnets.output_variable_name(0, "y", 1): "why?",
                wfnets.output_variable_name(1, "z", 1): "zee",
                wfnets.op_outputs_variable_name(0, 1): ["ex", "why?"],
                wfnets.op_outputs_variable_name(1, 1): ["zee"],
                }

        self.data_arcs = {
                2: {
                    0: {"X": "x", "Y": "y"},
                    1: {"Z": "z"},
                }}

        self.net = Mock()
        self.net.variable = Mock(wraps=self.variables.get)

        self.normal_token = Mock()
        type(self.normal_token.color_idx).value = PropertyMock(
                side_effect=rom.NotInRedisError("no"))

    def colored_token(self, color_idx):
        token = Mock()
        type(token.color_idx).value = PropertyMock(
                return_value=color_idx)
        return token

    def test_normal(self):
        expected_inputs = {"X": [1, 2, 3, 4], "Y": "y", "Z": "z"}

        input_conns = self.data_arcs[2]

        test_obj = TestableInputs(args={"input_connections": input_conns},
                tokens=[self.normal_token])
        inputs = test_obj.input_data(active_tokens_key=None, net=self.net)
        self.assertEqual(expected_inputs, inputs)

    def test_colored(self):
        expected_inputs = {"X": [5, 6, 7, 8], "Y": "why?", "Z": "zee"}

        input_conns = self.data_arcs[2]

        test_obj = TestableInputs(args={"input_connections": input_conns},
                tokens=[self.colored_token(1)])
        inputs = test_obj.input_data(active_tokens_key=None, net=self.net)
        self.assertEqual(expected_inputs, inputs)

    def test_null_connection_means_all(self):
        args = {"input_connections": {0: {}}}

        test_obj = TestableInputs(args=args, tokens=[self.normal_token])
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
        self.assertIsInstance(net.shortcut, exnets.ForkCommandNet)
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

        net = Mock()
        net.key = "netkey!"

        orchestrator = Mock()

        service_interfaces = {"orchestrator": orchestrator}
        wfnets.BuildParallelByAction.execute(obj,
                active_tokens_key=None, net=net, service_interfaces=service_interfaces)


if __name__ == "__main__":
    main()
