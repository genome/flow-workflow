from flow import petri

from flow_workflow.nets import io

import fakeredis
import unittest
import mock


class TestIo(unittest.TestCase):
    def setUp(self):
        self.conn = fakeredis.FakeRedis()
        self.services = {}

        self.token = petri.Token.create(self.conn)
        self.active_tokens_key = 'tokens'
        self.conn.lpush(self.active_tokens_key, self.token.key)

        self.net_variables = {}

        self.net = mock.Mock()
        self.net.key = 'netkey!'
        self.net.variable.side_effect = self.net_variables.get
        self.net.set_variable.side_effect = self.net_variables.__setitem__

    def test_store_outputs_action(self):
        op_id = 123
        args = {"operation_id": op_id}
        action = io.StoreOutputsAction.create(self.conn, args=args)

        outputs = {"a": "b", "c": "d"}
        self.token.data = {"exit_code": 0, "outputs": outputs}
        action.execute(self.active_tokens_key, self.net, self.services)

        var_a = io.output_variable_name(op_id, "a")
        var_c = io.output_variable_name(op_id, "c")
        var_output_names = io.op_outputs_variable_name(op_id)
        var_exit = io.op_exit_code_variable_name(op_id)

        expected = {
                var_a: "b",
                var_c: "d",
                var_output_names: ['a', 'c'],
                var_exit: 0,
                }

        self.assertEqual(expected, self.net_variables)


if __name__ == '__main__':
    unittest.main()
