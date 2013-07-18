from flow_workflow import future_operation
from flow_workflow import factory
from flow.petri_net import color
from flow.petri_net.net import Net
from flow_workflow.parallel_by import actions
from flow_workflow.parallel_id import ParallelIdentifier
from test_helpers import fakeredistest

import mock
import unittest


class PBSplitExecuteTest(fakeredistest.FakeRedisTest):
    def setUp(self):
        fakeredistest.FakeRedisTest.setUp(self)

        self.net = Net.create(self.conn)

        self.parallel_property = 'pfoo'
        self.parallel_input = ['a', 'b', 'c']
        self.create_operations()

        self.action = self.create_action()

        self.parent_color_group = self.net.add_color_group(size=1)
        self.parent_color_descriptor = color.ColorDescriptor(
                color=self.parent_color_group.begin,
                group=self.parent_color_group)

        self.service_interfaces = {}

    def create_operations(self):
        self.operation_id = 42
        self.input_connections = {
            7: {self.parallel_property: 'foosource'},
            3: {'bar': 'barsource'},
        }
        self.output_properties = []


        main_operation = self.create_operation(
                operation_id=self.operation_id,
                name='main operation',
                input_connections=self.input_connections,
                output_properties=self.output_properties)

        parallel_input_operation = self.create_operation(
                operation_id=7,
                name='parallel input operation',
                input_connections={},
                output_properties='foosource')
        parallel_input_operation.store_output(name='foosource',
                value=self.parallel_input, parallel_id=ParallelIdentifier())

        normal_input_operation = self.create_operation(
                operation_id=3,
                name='normal input operation',
                input_connections={},
                output_properties='barsource')
        normal_input_operation.store_output(name='barsource',
                value='bar value', parallel_id=ParallelIdentifier())

        self.operation = main_operation

    def create_operation(self, operation_id, name, **kwargs):
        fop = future_operation.FutureOperation(
                operation_class='direct_storage',
                operation_id=operation_id,
                name=name,
                parent=future_operation.NullFutureOperation(),
                log_dir='/exciting/log/dir',
                **kwargs)
        fop.save(self.net)

        return factory.load_operation(self.net, operation_id)


    def create_action(self):
        self.args = {
            'parallel_property': self.parallel_property,
            'operation_id': self.operation_id,
        }
        return actions.ParallelBySplit.create(self.conn, args=self.args)


    def test_execute(self):
        active_tokens = set()
        tokens, deferred = self.action.execute(net=self.net,
                color_descriptor=self.parent_color_descriptor,
                active_tokens=active_tokens,
                service_interfaces=self.service_interfaces)

        self.assertEqual(len(self.parallel_input), len(tokens))

        for i, val in enumerate(self.parallel_input):
            self.assertEqual(val,
                    self.operation.load_input(self.parallel_property,
                        parallel_id=ParallelIdentifier().child_identifier(
                            self.operation_id, i)))

class PBJoinExecuteTest(fakeredistest.FakeRedisTest):
    def setUp(self):
        fakeredistest.FakeRedisTest.setUp(self)

        self.net = Net.create(self.conn)

        self.parallel_property = 'pfoo'
        self.input_connections = {
            7: {self.parallel_property: 'foosource'},
        }
        self.output_properties = ['outfoo']
        self.parallel_output = ['o0', 'o1', 'o2']

        self.create_operations()

        self.action = self.create_action()

        self.color_group = self.net.add_color_group(
                size=len(self.parallel_output))
        self.color_descriptor = color.ColorDescriptor(
                color=self.color_group.begin,
                group=self.color_group)

        self.tokens = self.create_tokens()

        self.service_interfaces = {}

    def create_operations(self):
        self.operation_id = 42
        self.operation = self.create_operation(
                operation_id=self.operation_id,
                name='main operation',
                input_connections=self.input_connections,
                output_properties=self.parallel_property)

        for i, po in enumerate(self.parallel_output):
            self.operation.store_output(name=self.output_properties[0],
                    value=po,
                    parallel_id=ParallelIdentifier().child_identifier(
                        self.operation_id, i))

    def create_operation(self, operation_id, name, **kwargs):
        fop = future_operation.FutureOperation(
                operation_class='direct_storage',
                operation_id=operation_id,
                name=name,
                parent=future_operation.NullFutureOperation(),
                log_dir='/exciting/log/dir',
                **kwargs)
        fop.save(self.net)

        return factory.load_operation(self.net, operation_id)

    def create_action(self):
        self.args = {
            'output_properties': self.output_properties,
            'operation_id': self.operation_id,
        }
        return actions.ParallelByJoin.create(self.conn, args=self.args)

    def create_tokens(self):
        tokens = []
        for index, color in enumerate(self.color_group.colors):
            tokens.append(
                self.net.create_token(color=color,
                        color_group_idx=self.color_group.idx,
                        data=self.token_data_for(index)))
        return tokens

    def token_data_for(self, index):
        return {
            'workflow_data': {
                'parallel_id':
                    list(ParallelIdentifier().child_identifier(
                        self.operation_id, index))
            },
        }

    def test_execute(self):
        active_tokens = set(t.index for t in self.tokens)
        tokens, deferred = self.action.execute(net=self.net,
                color_descriptor=self.color_descriptor,
                active_tokens=active_tokens,
                service_interfaces=self.service_interfaces)
        self.assertEqual(1, len(tokens))

        self.assertEqual(['o0', 'o1', 'o2'],
                self.operation.load_output('outfoo', ParallelIdentifier()))


class ParallelBySplitTest(fakeredistest.FakeRedisTest):
    def setUp(self):
        fakeredistest.FakeRedisTest.setUp(self)

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
                connection=self.conn, key=self.key,
                args=self.args)

        self.parallel_id = ParallelIdentifier([[42, 3]])

        self.parent_color = 892
        self.parent_color_group = color.ColorGroup(
                idx=21, parent_color=502,
                parent_color_group_idx=11, begin=890, end=902)
        self.parent_color_descriptor = color.ColorDescriptor(
                color=self.parent_color, group=self.parent_color_group)

        self.operation = mock.MagicMock()


    def test_store_parallel_input(self):
        net = mock.MagicMock()
        net.key = 'netkey'
        parallel_input = ['a', 'b', 'c']

        self.action.store_parallel_input(operation=self.operation,
                parallel_input=parallel_input,
                parallel_property=self.parallel_property,
                parallel_id=self.parallel_id)

        self.assertEqual(3, self.operation.store_input.call_count)
        for pi in parallel_input:
            self.operation.store_input.assert_any_call(
                    name=self.parallel_property, value=pi,
                    parallel_id=mock.ANY)

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

class ParallelByJoinTest(fakeredistest.FakeRedisTest):
    def setUp(self):
        fakeredistest.FakeRedisTest.setUp(self)
        self.key = 'actionkey'

        self.operation_id = u'12345'
        self.output_properties = ['bar']

        self.args = {
            'output_properties': self.output_properties,
            'operation_id': self.operation_id,
        }

        self.action = actions.ParallelByJoin.create(
                connection=self.conn, key=self.key,
                args=self.args)

        self.net = mock.MagicMock()
        self.operation = mock.MagicMock()


#    def test_execute(self):
#        color_descriptor = mock.Mock()
#        active_tokens = mock.Mock()
#        service_interfaces = mock.MagicMock()
#
#        parallel_id = [(3, 2)]
#        workflow_data = {'parallel_id': parallel_id}
#
#        self.action.collect_array_output = mock.Mock()
#        self.action.collect_array_output.return_value = [mock.Mock()]
#
#        with mock.patch('flow_workflow.io') as m_io:
#            m_io.extract_workflow_data.return_value = workflow_data
#            self.action.execute(net=self.net,
#                    color_descriptor=color_descriptor,
#                    active_tokens=active_tokens,
#                    service_interfaces=service_interfaces)
#            m_io.extract_workflow_data.assert_called_once_with(self.net,
#                    active_tokens)
#            m_io.store_output.assert_called_once_with(net=self.net,
#                    operation_id=self.operation_id, property_name='bar',
#                    value=mock.ANY, parallel_id=mock.ANY)
#
#        self.net.create_token.assert_called_once()


    def test_collect_array_output(self):
        property_name = 'bar'
        parallel_size = 3
        parallel_id = ParallelIdentifier([(42, 7)])

        results = self.action.collect_array_output(net=self.net,
                property_name=property_name, parallel_size=parallel_size,
                operation=self.operation,
                parallel_id=parallel_id)

        self.assertEqual(parallel_size,
                len(self.operation.load_output.mock_calls))
        self.assertEqual([self.operation.load_output.return_value
            for x in xrange(parallel_size)],
                results)
        self.operation.load_output.assert_any_call(
                name=property_name, parallel_id=mock.ANY)


class ParallelByFailTest(fakeredistest.FakeRedisTest):
    def setUp(self):
        fakeredistest.FakeRedisTest.setUp(self)
        self.key = 'actionkey'

        self.action = actions.ParallelByFail.create(
                connection=self.conn, key=self.key)

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
