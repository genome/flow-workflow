from flow_workflow.pass_through.future_nets import PassThroughNet

import mock
import unittest


class CloneInputsNetTest(unittest.TestCase):
    def setUp(self):
        self.source_id = 1

        self.input_connections = {self.source_id: {'dst_name': 'src_name'}}
        self.name = 'foo'
        self.operation_id = 12345
        self.parent_operation_id = 54321

        self.net = PassThroughNet(
                input_connections=self.input_connections,
                name=self.name,
                operation_id=self.operation_id,
                parent_operation_id=self.parent_operation_id)


    def test_path(self):
        self.assertIn(self.net.skipping_place,
                self.net.internal_start_transition.arcs_out)
        self.assertIn(self.net.internal_success_transition,
                self.net.skipping_place.arcs_out)


if __name__ == "__main__":
    unittest.main()
