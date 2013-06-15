from flow_workflow.petri_net.future_nets import model

import mock
import unittest


class GenomeModelNetTest(unittest.TestCase):
    def setUp(self):
        self.operation_id = 12345
        self.parent_operation_id = 54321
        self.input_connections = {'foo': '456:bar'}


    def test_model_net(self):
        mod = model.GenomeModelNet(name='foo',
                operation_id=self.operation_id,
                parent_operation_id=self.parent_operation_id,
                input_connections=self.input_connections)
        self.assertTrue(False)


if __name__ == "__main__":
    unittest.main()
