from unittest import TestCase, main
from flow_workflow.historian.operation_data import OperationData

class OperationDataTests(TestCase):
    def setUp(self):
        self.net_key = 'some_net_key'
        self.operation_id = 999
        self.color = 888
        self.operation_data = OperationData(net_key=self.net_key,
                operation_id=self.operation_id,
                color=self.color)
        self.string = '{"color": 888, "net_key": "some_net_key", "operation_id": 999}'

    def test_to_dict(self):
        expected_dict = {
                'net_key':self.net_key,
                'operation_id':self.operation_id,
                'color':self.color,
                }
        self.assertItemsEqual(expected_dict, self.operation_data.to_dict)

    def test_from_dict(self):
        some_dict = {
                'net_key':'some_net_key',
                'operation_id':999,
                'color':888,
                }
        operation_data = OperationData.from_dict(some_dict)
        self.assertEqual(self.operation_data, operation_data)

    def test_dumps(self):
        self.assertEqual(self.string, self.operation_data.dumps())

    def test_loads(self):
        operation_data = OperationData.loads(self.string)
        self.assertEqual(self.operation_data, operation_data)

if __name__ == '__main__':
    main()
