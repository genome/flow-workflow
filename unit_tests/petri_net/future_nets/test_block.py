from flow_workflow.petri_net.future_nets import block

import mock
import unittest


class GenomeBlockNetTest(unittest.TestCase):
    def setUp(self):
        self.operation_id = 12345
        self.parent_operation_id = 54321
        self.source_id = 1
        self.input_connections = {self.source_id: {'dst_name': 'src_name'}}


    def test_block_net(self):
        mod = block.GenomeBlockNet(name='foo',
                operation_id=self.operation_id,
                parent_operation_id=self.parent_operation_id,
                input_connections=self.input_connections)

        self.assertTrue(False)


if __name__ == "__main__":
    unittest.main()
