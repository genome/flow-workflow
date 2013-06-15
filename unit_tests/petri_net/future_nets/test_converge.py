from flow_workflow.petri_net.future_nets import converge

import mock
import unittest


class GenomeConvergeNetTest(unittest.TestCase):
    def setUp(self):
        self.operation_id = 12345
        self.parent_operation_id = 54321
        self.source_id = 1
        self.input_connections = {self.source_id: {'dst_name': 'src_name'}}
        self.input_property_order = [self.source_id]
        self.output_properties = ['dst_name']


    def test_converge_net(self):
        mod = converge.GenomeConvergeNet(name='foo',
                operation_id=self.operation_id,
                parent_operation_id=self.parent_operation_id,
                input_connections=self.input_connections,
                input_property_order=self.input_property_order,
                output_properties=self.output_properties)

        self.assertTrue(False)


if __name__ == "__main__":
    unittest.main()
