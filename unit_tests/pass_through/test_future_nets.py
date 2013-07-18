from flow_workflow.pass_through.future_nets import PassThroughNet

import mock
import unittest


class PassThroughNetTest(unittest.TestCase):
    def setUp(self):
        self.net = PassThroughNet(name=None, operation_id=None)


    def test_path(self):
        self.assertIn(self.net.skipping_place,
                self.net.internal_start_transition.arcs_out)
        self.assertIn(self.net.internal_success_transition,
                self.net.skipping_place.arcs_out)


if __name__ == "__main__":
    unittest.main()
