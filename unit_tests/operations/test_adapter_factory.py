from flow_workflow.operations.adapter_factory import AdapterFactory
from lxml import etree
from mock import Mock
from unittest import TestCase

from flow_workflow.operations.perl_actions.adapters import (CommandAdapter,
        EventAdapter)
from flow_workflow.operations.converge.adapter import ConvergeAdapter

command_xml = etree.XML("""
<operation name="test">
    <operationtype commandClass="NullCommand" typeClass="Workflow::OperationType::Command" />
</operation>
""")

converge_xml = etree.XML("""
<operation name="test merge results">
  <operationtype typeClass="Workflow::OperationType::Converge">
    <inputproperty>result_1</inputproperty>
    <outputproperty>all_results</outputproperty>
    <outputproperty>result</outputproperty>
  </operationtype>
</operation>
""")

class Tests(TestCase):
    def test_init(self):
        operation_types = object()
        af = AdapterFactory(operation_types=operation_types)

        self.assertIs(af._operation_types, operation_types)
        self.assertEqual(af.next_operation_id, 0)

    def test_create_command_from_xml(self):
        expected_type_class = CommandAdapter
        af = AdapterFactory()

        return_value = af.create_from_xml(command_xml, log_dir='test_log_dir')
        self.assertIsInstance(return_value, CommandAdapter)

        self.assertEqual(return_value.name, 'test')
        self.assertEqual(return_value.operation_id, 0)
        self.assertEqual(return_value.xml, command_xml)
        self.assertEqual(return_value.log_dir, 'test_log_dir')

    def test_create_converge_from_xml(self):
        expected_type_class = CommandAdapter
        af = AdapterFactory()

        return_value = af.create_from_xml(converge_xml)
        self.assertIsInstance(return_value, ConvergeAdapter)

        self.assertEqual(return_value.name, 'test merge results')

    def test_create(self):
        expected_type_class = ConvergeAdapter
        af = AdapterFactory()

        return_value = af.create('Workflow::OperationType::Converge',
                log_dir='test_log_dir')
        self.assertIsInstance(return_value, ConvergeAdapter)

        self.assertEqual(return_value.operation_id, 0)
        self.assertEqual(return_value.xml, None)
        self.assertEqual(return_value.log_dir, 'test_log_dir')

    def test_failed_create(self):
        af = AdapterFactory()
        with self.assertRaises(ValueError):
            af.create('does not exist')
