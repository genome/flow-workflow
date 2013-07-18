from flow_workflow.entities.converge import future_nets

import mock
import unittest


class ConvergeNetTest(unittest.TestCase):
    def setUp(self):
        self.source_id = 1

        self.name = 'converinator'
        self.input_property_order = ['foo', 'bar', 'baz']
        self.operation_id = 12345
        self.output_properties = None

        self.net = future_nets.ConvergeNet(name=self.name,
                operation_id=self.operation_id,
                output_properties=self.output_properties,
                input_property_order=self.input_property_order)


    def test_path(self):
        self.assertIn(self.net.starting_place,
                self.net.internal_start_transition.arcs_out)
        self.assertIn(self.net.converge_transition,
                self.net.starting_place.arcs_out)
        self.assertIn(self.net.succeeding_place,
                self.net.converge_transition.arcs_out)
        self.assertIn(self.net.internal_success_transition,
                self.net.succeeding_place.arcs_out)


if __name__ == "__main__":
    unittest.main()
