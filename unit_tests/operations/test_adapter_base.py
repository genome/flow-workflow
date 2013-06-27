from flow_workflow.operations import adapter_base
from lxml import etree
from mock import Mock
from unittest import TestCase

import os


xml = etree.XML("""
<operation name="test">
    <operationtype commandClass="NullCommand" typeClass="Workflow::OperationType::Command" />
</operation>
""")

bad_xml = etree.XML("""
<operation name="bad">
    <operationtype commandClass="NullCommand" typeClass="Workflow::OperationType::Command" />
    <operationtype commandClass="NullCommand" typeClass="Workflow::OperationType::Command" />
</operation>
""")

class Tests(TestCase):
    def test_clean_log_file_name(self):
        result = adapter_base.clean_log_file_name(" @#$  hi  )%^*@#  .log")
        self.assertEqual("hi_.log", result)

    def setUp(self):
        self.adapter_factory = object()
        self.operation_id = 99
        self.log_dir = 'test_dir'
        self.parent = Mock()
        self.parent.operation_id = None

        self.ab = adapter_base.AdapterBase(
                adapter_factory=self.adapter_factory,
                operation_id=self.operation_id,
                xml=xml,
                log_dir=self.log_dir,
                parent=self.parent)

    def test_init(self):
        self.assertIs(self.ab.adapter_factory, self.adapter_factory)
        self.assertIs(self.ab.operation_id, self.operation_id)
        self.assertIs(self.ab.parent, self.parent)
        self.assertIs(self.ab.parent_id, None)
        self.assertIs(self.ab.xml, xml)
        self.assertEqual(self.ab.name, 'test')
        self.assertIs(self.ab.log_dir, self.log_dir)
        self.assertEqual(self.ab.stdout_log_file, 'test_dir/test.99.out')
        self.assertEqual(self.ab.stderr_log_file, 'test_dir/test.99.err')

        self.parent.operation_id = object()
        ab = adapter_base.AdapterBase(
                adapter_factory=self.adapter_factory,
                operation_id=self.operation_id,
                xml=xml,
                log_dir=self.log_dir,
                parent=self.parent)
        self.assertIs(ab.parent_id, self.parent.operation_id)

    def test_parse_xml(self):
        name, type_node, operation_attributes, type_attributes = \
                adapter_base.parse_xml(xml)
        self.assertEqual(name, 'test')

        expected_oa = {'name': 'test'}
        expected_ta = {'typeClass': 'Workflow::OperationType::Command',
                'commandClass': 'NullCommand'}
        self.assertEqual(operation_attributes, expected_oa)
        self.assertEqual(type_attributes, expected_ta)

    def test_parse_bad_xml(self):
        with self.assertRaises(ValueError):
            adapter_base.parse_xml(bad_xml)

    def test_determine_log_paths(self):
        name = 'test_name'
        operation_id = 'test_op_id'
        log_dir = 'test_log_dir'
        stdout, stderr = adapter_base.determine_log_paths(name,
                operation_id, log_dir)

        out = ('test_log_dir', 'test_name.test_op_id.out')
        err = ('test_log_dir', 'test_name.test_op_id.err')
        self.assertEqual(os.path.split(stdout), out)
        self.assertEqual(os.path.split(stderr), err)

    def test_net(self):
        with self.assertRaises(NotImplementedError):
            self.ab.net(None)

    def test_children(self):
        self.assertEqual(self.ab.children, [])
