from flow_workflow.clone_inputs_future_net import CloneInputsNet
from flow_workflow.entities.model import adapter
from flow_workflow.future_operation import NullFutureOperation
from flow_workflow.perl_action.future_nets import PerlActionNet
from lxml import etree

import mock
import unittest
import flow_workflow.adapter_base


VALID_XML = '''
<operation name="test_model" logDir="/tmp/test/log/dir">
  <operation name="A">
    <operationtype commandClass="NullCommand"
                   typeClass="Workflow::OperationType::Command" />
  </operation>
  <operation name="B">
    <operationtype commandClass="NullCommand"
                   typeClass="Workflow::OperationType::Command" />
  </operation>

  <link fromOperation="input connector" fromProperty="a"
        toOperation="A" toProperty="param" />
  <link fromOperation="input connector" fromProperty="b"
        toOperation="B" toProperty="param" />
  <link fromOperation="A" fromProperty="result"
        toOperation="output connector" toProperty="out_a" />
  <link fromOperation="B" fromProperty="result"
        toOperation="output connector" toProperty="out_b" />

  <operationtype typeClass="Workflow::OperationType::Model">
    <inputproperty>prior_result</inputproperty>
    <outputproperty>result</outputproperty>
  </operationtype>
</operation>
'''


class ModelAdapterTest(unittest.TestCase):
    def setUp(self):
        self.operation_id = 12345
        self.adapter = adapter.ModelAdapter(xml=etree.XML(VALID_XML),
                operation_id=self.operation_id,
                parent=flow_workflow.adapter_base.NullAdapter())

    def test_children(self):
        self.assertItemsEqual(['input connector', 'output connector', 'A', 'B'],
                [c.name for c in self.adapter.children])

    def test_links(self):
        self.assertItemsEqual(['A', 'B', 'input connector', 'input connector'],
                [l.from_operation for l in self.adapter.links])
        self.assertItemsEqual(['A', 'B', 'output connector',
            'output connector'], [l.to_operation for l in self.adapter.links])

    def test_data_arcs(self):
        expected_data_arcs = {
            'A': {'input connector': {'param': 'a'}},
            'B': {'input connector': {'param': 'b'}},
            'output connector': {
                'A': {'out_a': 'result'},
                'B': {'out_b': 'result'},
            }
        }
        self.assertEqual(expected_data_arcs, self.adapter.data_arcs)

    def test_edges(self):
        expected_edges = {
            'input connector': {'A', 'B'},
            'A': {'output connector'},
            'B': {'output connector'},
        }
        self.assertEqual(expected_edges, self.adapter.edges)

    def test_child_input_connections(self):
        input_connections = mock.Mock()
        ic_operation_id = self.adapter.child_operation_id('input connector')
        expected_input_connections = {
            ic_operation_id: {
                'param': 'a' }
        }

        self.assertEqual(expected_input_connections,
                self.adapter.child_input_connections('A', input_connections))

    def test_child_input_connections_input_connector(self):
        input_connections = mock.Mock()
        self.assertEqual(input_connections,
                self.adapter.child_input_connections('input connector',
                    input_connections))

    def test_child_output_properties(self):
        output_props = mock.Mock()
        expected_output_properties = ['result']

        self.assertEqual(expected_output_properties,
                self.adapter.child_output_properties('A', output_props))

    def test_child_output_properties_output_connector(self):
        output_props = mock.Mock()
        self.assertEqual(output_props, self.adapter.child_output_properties(
            'output connector', output_props))

    def test_subnets(self):
        input_connections = {}
        output_properties = []
        resources = {}
        child_nets = self.adapter.subnets(input_connections, output_properties,
                resources)
        self.assertItemsEqual(['A', 'B', 'input connector', 'output connector'],
                child_nets.keys())
        self.assertItemsEqual([CloneInputsNet, CloneInputsNet, PerlActionNet,
            PerlActionNet], [c.__class__ for c in child_nets.itervalues()])

    def get_subnet(self, net, name):
        for subnet in net.subnets:
            if name == subnet.name:
                return subnet

    def ensure_singly_connected(self, src, dest):
        all_connections = set()
        for intermediate in src.success_transition.arcs_out:
            all_connections = all_connections.union(intermediate.arcs_out)

        self.assertIn(dest.start_transition, all_connections)

    def test_future_net(self):
        resources = {}
        output_properties = []
        input_connections = {}
        net = self.adapter.future_net(input_connections, output_properties,
                resources)

        ic = self.get_subnet(net, 'input connector')
        oc = self.get_subnet(net, 'output connector')
        a = self.get_subnet(net, 'A')
        b = self.get_subnet(net, 'B')

        # Start
        self.assertIn(net.starting_place,
                net.internal_start_transition.arcs_out)
        self.assertIn(ic.start_transition, net.starting_place.arcs_out)

        # Success
        self.assertIn(net.succeeding_place, oc.success_transition.arcs_out)
        self.assertIn(net.success_transition, net.succeeding_place.arcs_out)

        # Failure
        self.assertIn(net.failure_transition, net.failing_place.arcs_out)
        self.assertIn(net.failing_place, a.failure_transition.arcs_out)
        self.assertIn(net.failing_place, b.failure_transition.arcs_out)

        # Internal links
        self.ensure_singly_connected(ic, a)
        self.ensure_singly_connected(ic, b)
        self.ensure_singly_connected(a, oc)
        self.ensure_singly_connected(b, oc)

    def test_future_operations(self):
        future_ops = self.adapter.future_operations(NullFutureOperation(),
                input_connections=mock.Mock(),
                output_properties=mock.Mock())
        self.assertEqual(5, len(future_ops))


if __name__ == "__main__":
    unittest.main()
