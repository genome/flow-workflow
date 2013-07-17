from flow.petri_net import color
from flow_workflow.parallel_by import actions
from flow_workflow.parallel_id import ParallelIdentifier

import fakeredis
import mock
import unittest


class ParallelBySplitTest(unittest.TestCase):
    def setUp(self):
        self.connection = fakeredis.FakeRedis()
        self.connection.flushall()
        self.key = 'actionkey'
        self.input_connections = {u'7': {u'pfoo': u'sbar'}}
        self.operation_id = 12345
        self.parallel_property = 'pfoo'

        self.args = {
            'input_connections': self.input_connections,
            'operation_id': self.operation_id,
            'parallel_property': self.parallel_property,
        }

        self.action = actions.ParallelBySplit.create(
                connection=self.connection, key=self.key,
                args=self.args)

        self.parallel_id = ParallelIdentifier([[42, 3]])

        self.parent_color = 892
        self.parent_color_group = color.ColorGroup(
                idx=21, parent_color=502,
                parent_color_group_idx=11, begin=890, end=902)
        self.parent_color_descriptor = color.ColorDescriptor(
                color=self.parent_color, group=self.parent_color_group)

    def tearDown(self):
        self.connection.flushall()

    def test_execute(self):
        net = mock.Mock()
        active_tokens = [mock.Mock()]
        service_interfaces = mock.MagicMock()

        self.action.store_parallel_input = mock.Mock()
        self.action._create_tokens = mock.Mock()
        with mock.patch('flow_workflow.parallel_by.actions.io') as m_io:
            parallel_input = ['a', 'b', 'c']
            m_io.load_input.return_value = parallel_input
            self.action.execute(net=net,
                    color_descriptor=self.parent_color_descriptor,
                    active_tokens=active_tokens,
                    service_interfaces=service_interfaces)

            m_io.extract_workflow_data.assert_called_once_with(net,
                    active_tokens)
            m_io.load_input.assert_called_once_with(net=net,
                    input_connections=self.input_connections,
                    property_name=self.parallel_property,
                    parallel_id=mock.ANY)
            self.action.store_parallel_input.assert_called_once_with(
                    net, parallel_input, mock.ANY)
            self.action._create_tokens.assert_called_once_with(net=net,
                    num_tokens=3, color_descriptor=self.parent_color_descriptor,
                    workflow_data=mock.ANY)


    def test_store_parallel_input(self):
        net = mock.Mock()
        net.key = 'netkey'
        parallel_input = ['a', 'b', 'c']

        with mock.patch('flow_workflow.io.store_output') as store:
            self.action.store_parallel_input(net, parallel_input,
                    self.parallel_id)
            self.assertEqual(len(parallel_input), len(store.mock_calls))

            for val in parallel_input:
                store.assert_any_call(operation_id=u'7', value=val, net=net,
                        property_name='sbar', parallel_id=mock.ANY)

    def test_determine_input_source_success(self):
        self.assertEqual((u'7', u'sbar'),  # Fakeredis makes these unicode..
                self.action.determine_input_source('pfoo'))

    def test_determine_input_source_error(self):
        with self.assertRaises(KeyError):
            self.action.determine_input_source('nonsense_property')

    def test_create_tokens(self):
        color_group = color.ColorGroup(idx=27, parent_color=892,
                parent_color_group_idx=21, begin=1022, end=1029)
        num_tokens = 7
        workflow_data = {'parallel_id': [[42, 3]]}

        net = mock.Mock()
        net.add_color_group.return_value = color_group

        tokens = self.action._create_tokens(num_tokens=num_tokens,
                color_descriptor=self.parent_color_descriptor,
                workflow_data=workflow_data, net=net)

        self.assertEqual(num_tokens, len(tokens))
        self.assertEqual(num_tokens, len(net.create_token.mock_calls))
        net.create_token.assert_any_call(color=color_group.begin,
                color_group_idx=color_group.idx, data=mock.ANY)


class ParallelByJoinTest(unittest.TestCase):
    def setUp(self):
        self.connection = fakeredis.FakeRedis()
        self.connection.flushall()
        self.key = 'actionkey'

        self.operation_id = u'12345'
        self.output_properties = ['bar']

        self.args = {
            'output_properties': self.output_properties,
            'operation_id': self.operation_id,
        }

        self.action = actions.ParallelByJoin.create(
                connection=self.connection, key=self.key,
                args=self.args)

        self.net = mock.Mock()

    def tearDown(self):
        self.connection.flushall()

    def test_execute(self):
        color_descriptor = mock.Mock()
        active_tokens = mock.Mock()
        service_interfaces = mock.MagicMock()

        parallel_id = [(3, 2)]
        workflow_data = {'parallel_id': parallel_id}

        self.action.collect_array_output = mock.Mock()
        self.action.collect_array_output.return_value = [mock.Mock()]

        with mock.patch('flow_workflow.parallel_by.actions.io') as m_io:
            m_io.extract_workflow_data.return_value = workflow_data
            self.action.execute(net=self.net,
                    color_descriptor=color_descriptor,
                    active_tokens=active_tokens,
                    service_interfaces=service_interfaces)
            m_io.extract_workflow_data.assert_called_once_with(self.net,
                    active_tokens)
            m_io.store_output.assert_called_once_with(net=self.net,
                    operation_id=self.operation_id, property_name='bar',
                    value=mock.ANY, parallel_id=mock.ANY)

        self.net.create_token.assert_called_once()


    def test_collect_array_output(self):
        property_name = 'bar'
        parallel_size = 3
        parallel_id = ParallelIdentifier([(42, 7)])

        with mock.patch('flow_workflow.io.load_output') as load:
            results = self.action.collect_array_output(net=self.net,
                    property_name=property_name, parallel_size=parallel_size,
                    operation_id=self.operation_id,
                    parallel_id=parallel_id)

            self.assertEqual(parallel_size, len(load.mock_calls))
            self.assertEqual([load.return_value for x in xrange(parallel_size)],
                    results)
            load.assert_any_call(net=self.net, operation_id=self.operation_id,
                    property_name=property_name, parallel_id=mock.ANY)


class ParallelByFailTest(unittest.TestCase):
    def setUp(self):
        self.connection = fakeredis.FakeRedis()
        self.connection.flushall()
        self.key = 'actionkey'

        self.action = actions.ParallelByFail.create(
                connection=self.connection, key=self.key)

    def tearDown(self):
        self.connection.flushall()

    def test_execute(self):
        net = mock.Mock()
        color_descriptor = mock.Mock()
        active_tokens = mock.Mock()
        service_interfaces = mock.MagicMock()

        with mock.patch('flow_workflow.io.extract_workflow_data') as wf_data:
            tokens, deferred = self.action.execute(net=net,
                    color_descriptor=color_descriptor,
                    active_tokens=active_tokens,
                    service_interfaces=service_interfaces)
            wf_data.assert_called_once_with(net, active_tokens)

        net.create_token.assert_called_once_with(
                color=color_descriptor.group.parent_color,
                color_group_idx=color_descriptor.group.parent_color_group_idx,
                data=mock.ANY)
        self.assertEqual(1, len(tokens))


if __name__ == '__main__':
    unittest.main()
