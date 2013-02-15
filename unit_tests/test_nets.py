import flow_workflow.nets as wfnets

from unittest import TestCase, main
import flow.petri.netbuilder as nb
import flow.command_runner.executors.nets as exnets


class NetTest(TestCase):
    def setUp(self):
        self.builder = nb.NetBuilder()


class TestGenomeActionNet(NetTest):
    def test_genome_shortcut_action(self):
        input_conns = {123: {"x": "y"}}

        net = wfnets.GenomeActionNet(self.builder,
                name="test",
                operation_id=6,
                input_connections=input_conns,
                action_type="command",
                action_id="ClassX")

        self.assertIsInstance(net.start_transition, nb.Transition)
        self.assertIsInstance(net.success_transition, nb.Transition)
        self.assertIsInstance(net.failure_transition, nb.Transition)
        self.assertIsInstance(net.shortcut, exnets.LocalCommandNet)
        self.assertIsInstance(net.execute, exnets.LSFCommandNet)


if __name__ == "__main__":
    main()
