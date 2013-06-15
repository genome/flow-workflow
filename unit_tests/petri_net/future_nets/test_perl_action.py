from flow_workflow.petri_net.future_nets import perl_action

import mock
import unittest


class PerlActionNetTest(unittest.TestCase):
    def setUp(self):
        self.operation_id = 12345
        self.input_connections = {'foo': '456:bar'}
        self.stderr = '/tmp/foo.err'
        self.stdout = '/tmp/foo.out'
        self.resources = {}


    def test_command(self):
        com = perl_action.GenomeCommandNet(name='foo',
                operation_id=self.operation_id,
                input_connections=self.input_connections,
                stderr=self.stderr, stdout=self.stdout,
                resources=self.resources)
        self.assertTrue(False)


if __name__ == "__main__":
    unittest.main()
