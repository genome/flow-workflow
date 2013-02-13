import flow_workflow.xmladapter as wfxml

from lxml import etree
from lxml.builder import E

from unittest import TestCase, main
import flow.petri.netbuilder as nb
import json

serial_xml = """<?xml version='1.0' standalone='yes'?>
<workflow name="Test workflow" executor="Workflow::Executor::SerialDeferred">
  <link fromOperation="input connector" fromProperty="input" toOperation="job_1" toProperty="param" />
  <link fromOperation="job_1" fromProperty="result" toOperation="job_2" toProperty="param" />
  <link fromOperation="job_2" fromProperty="result" toOperation="job_3" toProperty="param" />
  <link fromOperation="job_3" fromProperty="result" toOperation="output connector" toProperty="output" />
  <operation name="job_1">
    <operationtype commandClass="NullCommand" lsfQueue="short" typeClass="Workflow::OperationType::Command" />
  </operation>
  <operation name="job_2">
    <operationtype commandClass="NullCommand" lsfQueue="short" typeClass="Workflow::OperationType::Command" />
  </operation>
  <operation name="job_3">
    <operationtype commandClass="NullCommand" lsfQueue="short" typeClass="Workflow::OperationType::Command" />
  </operation>
  <operationtype typeClass="Workflow::OperationType::Model">
    <inputproperty>input</inputproperty>
    <outputproperty>output</outputproperty>
  </operationtype>
</workflow>
"""

wf_command_class = "Workflow::OperationType::Command"
wf_event_class = "Workflow::OperationType::Event"
wf_converge_class = "Workflow::OperationType::Converge"

def _make_op_xml(op_attr, type_attr):
    return E.operation(op_attr, E.operationtype(type_attr))


def _make_command_op_xml(name, perl_class, op_attr=None, type_attr=None):
    if op_attr is None: op_attr = {}
    if type_attr is None: type_attr = {}

    op_attr['name'] = name
    type_attr['commandClass'] = perl_class
    type_attr['typeClass'] = wf_command_class

    return _make_op_xml(op_attr, type_attr)


def _make_event_op_xml(name, event_id, op_attr={}, type_attr={}):
    if op_attr is None: op_attr = {}
    if type_attr is None: type_attr = {}

    op_attr['name'] = name
    type_attr['eventId'] = event_id

    return _make_op_xml(op_attr, type_attr)


def _make_converge_op_xml(name, inputs, outputs):
    subelts = ([E.inputproperty(x) for x in inputs] +
            [E.outputproperty(x) for x in outputs])

    return E.operation({"name": name},
            E.operationtype({"typeClass": wf_converge_class}, *subelts))


class TestWorkflowEntity(TestCase):
    def test_abstract_net(self):
        builder = nb.NetBuilder("test")
        entity = wfxml.WorkflowEntity(job_number=0)
        self.assertRaises(NotImplementedError, entity.net, builder)


class TestWorkflowOperations(TestCase):
    def test_no_operationtype_tag(self):
        tree = E.operation({"name": "badguy"})
        self.assertRaises(ValueError, wfxml.WorkflowOperation, job_number=0,
                log_dir="/tmp", xml=tree)

    def test_multiple_operationtype_tags(self):
        type_attr = {"commandClass": "A", "typeClass": wf_command_class}
        tree = E.operation({"name": "badguy"},
                E.operationtype(type_attr),
                E.operationtype(type_attr)
        )
        self.assertRaises(ValueError, wfxml.WorkflowOperation, job_number=0,
                log_dir="/tmp", xml=tree)

    def test_command(self):
        tree = _make_command_op_xml(name="op nums 1/2", perl_class="X",
                op_attr={"a": "b"}, type_attr={"x": "y"})

        op = wfxml.CommandOperation(job_number=4, log_dir="/tmp", xml=tree)
        self.assertEqual(4, op.job_number)
        self.assertEqual("op nums 1/2", op.name)
        self.assertEqual("b", op._operation_attributes["a"])
        self.assertEqual("y", op._type_attributes["x"])

        self.assertEqual("/tmp", op.log_dir)
        self.assertEqual("/tmp/4-op_nums_1_2.out", op.stdout_log_file)
        self.assertEqual("/tmp/4-op_nums_1_2.err", op.stderr_log_file)

        self.assertEqual("X", op.perl_class)
        self.assertEqual("", op.parallel_by)

    def test_parallel_by_command(self):
        tree = _make_command_op_xml(name="pby", perl_class="X",
            op_attr={"parallelBy": "input_file"})

        op = wfxml.CommandOperation(job_number=4, log_dir="/tmp", xml=tree)
        self.assertEqual("input_file", op.parallel_by)

    def test_event(self):
        tree = _make_event_op_xml(name="evt", event_id="123")

        op = wfxml.EventOperation(job_number=4, log_dir="/tmp", xml=tree)
        self.assertEqual(4, op.job_number)
        self.assertEqual("evt", op.name)
        self.assertEqual("123", op.event_id)

    def test_converge_exceptions(self):
        tree = _make_converge_op_xml(name="merge", inputs=[], outputs=["x"])
        self.assertRaises(ValueError, wfxml.ConvergeOperation, job_number=4,
                log_dir="/tmp", xml=tree)

        tree = _make_converge_op_xml(name="merge", inputs=["x"], outputs=[])
        self.assertRaises(ValueError, wfxml.ConvergeOperation, job_number=4,
                log_dir="/tmp", xml=tree)

    def test_converge(self):
        inputs = ["a", "b", "c"]
        outputs = ["x", "y"]

        tree = _make_converge_op_xml(name="merge", inputs=inputs,
                outputs=outputs)

        op = wfxml.ConvergeOperation(job_number=4, log_dir="/tmp", xml=tree)
        self.assertEqual(4, op.job_number)
        self.assertEqual("merge", op.name)
        self.assertEqual(inputs, op.input_properties)
        self.assertEqual(outputs, op.output_properties)


class TestXmlAdapter(TestCase):
    def test_converge(self):
        xml = """<operation name="TestConverge">
    <operationtype typeClass="Workflow::OperationType::Converge">
        <inputproperty>1_of_3_merge_operation</inputproperty>
        <inputproperty>2_of_3_merge_operation</inputproperty>
        <inputproperty>3_of_3_merge_operation</inputproperty>
        <outputproperty>output1</outputproperty>
        <outputproperty>output2</outputproperty>
    </operationtype>
</operation>
"""
        tree = etree.XML(xml)
        op = wfxml.ConvergeOperation(3, "/tmp", tree)
        self.assertEqual(3, op.job_number)
        self.assertEqual("TestConverge", op.name)
        self.assertEqual("/tmp", op.log_dir)
        self.assertEqual("/tmp/3-TestConverge.out", op.stdout_log_file)
        self.assertEqual("/tmp/3-TestConverge.err", op.stderr_log_file)

        self.assertEqual(
            ["%d_of_3_merge_operation" %x for x in xrange(1,4)],
            op.input_properties)
        self.assertEqual(["output1", "output2"], op.output_properties)

    def test_command(self):
        xml = """<operation name="TestCommand">
    <operationtype commandClass="A::Command" typeClass="Workflow::OperationType::Command" />
</operation>
"""
        tree = etree.XML(xml)
        op = wfxml.CommandOperation(2, "/tmp", tree)
        self.assertEqual(2, op.job_number)
        self.assertEqual("TestCommand", op.name)
        self.assertEqual("/tmp", op.log_dir)
        self.assertEqual("/tmp/2-TestCommand.out", op.stdout_log_file)
        self.assertEqual("/tmp/2-TestCommand.err", op.stderr_log_file)

        self.assertEqual("A::Command", op.perl_class)
        self.assertEqual("", op.parallel_by)

    def test_command_parallel_by(self):
        xml = """<operation name="TestParallel" parallelBy="builds">
    <operationtype commandClass="Another::Command" typeClass="Workflow::OperationType::Command" />
</operation>
"""

        tree = etree.XML(xml)
        op = wfxml.CommandOperation(2, "/tmp", tree)
        self.assertEqual(2, op.job_number)
        self.assertEqual("TestParallel", op.name)
        self.assertEqual("/tmp", op.log_dir)
        self.assertEqual("/tmp/2-TestParallel.out", op.stdout_log_file)
        self.assertEqual("/tmp/2-TestParallel.err", op.stderr_log_file)

        self.assertEqual("Another::Command", op.perl_class)
        self.assertEqual("builds", op.parallel_by)


    def test_serial(self):
        model = wfxml.convert_workflow_xml(serial_xml)
        self.assertEqual("Test workflow", model.name)

        self.assertEqual(5, len(model.operations))
        expected_names = [ "input connector", "output connector" ]
        expected_names.extend(("job_%d" %x for x in xrange(1,4)))
        self.assertEqual(expected_names, [x.name for x in model.operations])
        self.assertEqual(wfxml.InputConnector, type(model.operations[0]))
        self.assertEqual(wfxml.OutputConnector, type(model.operations[1]))
        self.assertEqual([wfxml.CommandOperation]*3,
                         [type(x) for x in model.operations[2:]])

        self.assertEqual(range(0,5), [x.job_number for x in model.operations])
        self.assertEqual(set([2]), model.edges[0])
        self.assertTrue(1 not in model.edges)
        self.assertEqual(set([3]), model.edges[2])
        self.assertEqual(set([4]), model.edges[3])
        self.assertEqual(set([1]), model.edges[4])

        self.assertTrue(0 not in model.rev_edges)
        self.assertEqual(set([4]), model.rev_edges[1])
        self.assertEqual(set([0]), model.rev_edges[2])
        self.assertEqual(set([2]), model.rev_edges[3])
        self.assertEqual(set([3]), model.rev_edges[4])

if __name__ == "__main__":
    main()
