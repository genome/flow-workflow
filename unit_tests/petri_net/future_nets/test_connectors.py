from flow_workflow.petri_net.future_nets import connectors

import mock
import unittest


class GenomeInputConnectorNetTest(unittest.TestCase):
    def setUp(self):
        self.operation_id = 12345
        self.parent_operation_id = 54321
        self.source_id = 1
        self.input_connections = {self.source_id: {'dst_name': 'src_name'}}

    def test_input_connector_net(self):
        mod = connectors.GenomeInputConnectorNet(name='foo',
                operation_id=self.operation_id,
                parent_operation_id=self.parent_operation_id,
                input_connections=self.input_connections)

        self.assertTrue(False)


class GenomeOutputConnectorNetTest(unittest.TestCase):
    def setUp(self):
        self.operation_id = 12345
        self.parent_operation_id = 54321
        self.source_id = 1
        self.input_connections = {self.source_id: {'dst_name': 'src_name'}}

    def test_output_connector_net(self):
        mod = connectors.GenomeOutputConnectorNet(name='foo',
                operation_id=self.operation_id,
                parent_operation_id=self.parent_operation_id,
                input_connections=self.input_connections)

        self.assertTrue(False)


if __name__ == "__main__":
    unittest.main()
