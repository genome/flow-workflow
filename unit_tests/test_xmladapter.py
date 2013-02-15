import flow_workflow.xmladapter as wfxml
import flow_workflow.nets as wfnets

from lxml import etree
from lxml.builder import E

from unittest import TestCase, main
import flow.petri.netbuilder as nb
import flow.command_runner.executors.nets as exnets

_wf_command_class = "Workflow::OperationType::Command"
_wf_converge_class = "Workflow::OperationType::Converge"
_wf_event_class = "Workflow::OperationType::Event"
_wf_model_class = "Workflow::OperationType::Model"

def _link_xml(from_op, from_prop, to_op, to_prop):
    return E.link({
        "fromOperation": from_op,
        "fromProperty": from_prop,
        "toOperation": to_op,
        "toProperty": to_prop,
    })

def _op_xml(op_attr, type_attr):
    return E.operation(op_attr, E.operationtype(type_attr))


def _command_op_xml(name, perl_class, op_attr=None, type_attr=None):
    if op_attr is None: op_attr = {}
    if type_attr is None: type_attr = {}

    op_attr['name'] = name
    type_attr['commandClass'] = perl_class
    type_attr['typeClass'] = _wf_command_class

    return _op_xml(op_attr, type_attr)


def _event_op_xml(name, event_id, op_attr={}, type_attr={}):
    if op_attr is None: op_attr = {}
    if type_attr is None: type_attr = {}

    op_attr['name'] = name
    type_attr['eventId'] = event_id

    return _op_xml(op_attr, type_attr)


def _converge_op_xml(name, inputs, outputs):
    subelts = ([E.inputproperty(x) for x in inputs] +
            [E.outputproperty(x) for x in outputs])

    return E.operation({"name": name},
            E.operationtype({"typeClass": _wf_converge_class}, *subelts))


class TestWorkflowEntity(TestCase):
    def test_abstract_net(self):
        builder = nb.NetBuilder()
        entity = wfxml.WorkflowEntity()
        self.assertRaises(NotImplementedError, entity.net, builder)


class TestWorkflowOperations(TestCase):
    def setUp(self):
        self.builder = nb.NetBuilder()

    def test_no_operationtype_tag(self):
        tree = E.operation({"name": "badguy"})
        self.assertRaises(ValueError,
                wfxml.WorkflowOperation, log_dir="/tmp", xml=tree)

    def test_multiple_operationtype_tags(self):
        type_attr = {"commandClass": "A", "typeClass": _wf_command_class}
        tree = E.operation({"name": "badguy"},
                E.operationtype(type_attr),
                E.operationtype(type_attr)
        )
        self.assertRaises(ValueError,
                wfxml.WorkflowOperation, log_dir="/tmp", xml=tree)

    def test_command(self):
        tree = _command_op_xml(name="op nums 1/2", perl_class="ClassX",
                op_attr={"a": "b"}, type_attr={"x": "y"})

        op = wfxml.CommandOperation(log_dir="/tmp", xml=tree)
        self.assertEqual("op nums 1/2", op.name)
        self.assertEqual("b", op._operation_attributes["a"])
        self.assertEqual("y", op._type_attributes["x"])

        self.assertEqual("/tmp", op.log_dir)
        self.assertRegexpMatches(op.stdout_log_file,
                "/tmp/[0-9]+-op_nums_1_2.out")
        self.assertRegexpMatches(op.stderr_log_file,
                "/tmp/[0-9]+-op_nums_1_2.err")

        self.assertEqual("ClassX", op.perl_class)
        self.assertEqual("", op.parallel_by)

        input_conns = {0: {"x": "y"}}

        net = op.net(self.builder, input_connections=input_conns)
        self.assertIsInstance(net, wfnets.GenomeActionNet)
        self.assertEqual("command", net.action_type)
        self.assertEqual("ClassX", net.action_id)

        self.assertIsInstance(net.shortcut, exnets.LocalCommandNet)
        shortcut_transition = net.shortcut.dispatch
        action = shortcut_transition.action
        self.assertIsInstance(action, nb.ActionSpec)
        self.assertEqual(wfnets.GenomeShortcutAction, action.cls)

        flat_input_conns = wfnets._flatten_input_connections(input_conns)
        expected_args = {
                "action_type": "command",
                "action_id": "ClassX",
                "with_outputs": True,
                "input_connections": flat_input_conns,
        }
        self.assertDictContainsSubset(expected_args, action.args)

        self.assertIsInstance(net.execute, exnets.LSFCommandNet)
        execute_transition = net.execute.dispatch
        action = execute_transition.action
        self.assertIsInstance(action, nb.ActionSpec)
        self.assertEqual(wfnets.GenomeExecuteAction, action.cls)


    def test_parallel_by_command(self):
        tree = _command_op_xml(name="pby", perl_class="X",
            op_attr={"parallelBy": "input_file"})

        op = wfxml.CommandOperation(log_dir="/tmp", xml=tree)
        self.assertEqual("input_file", op.parallel_by)

    def test_event(self):
        tree = _event_op_xml(name="evt", event_id="123")

        op = wfxml.EventOperation(log_dir="/tmp", xml=tree)
        self.assertEqual("evt", op.name)
        self.assertEqual("123", op.event_id)

    def test_converge_exceptions(self):
        tree = _converge_op_xml(name="merge", inputs=[], outputs=["x"])
        self.assertRaises(ValueError,
                wfxml.ConvergeOperation, log_dir="/tmp", xml=tree)

        tree = _converge_op_xml(name="merge", inputs=["x"], outputs=[])
        self.assertRaises(ValueError,
                wfxml.ConvergeOperation, log_dir="/tmp", xml=tree)

    def test_converge(self):
        inputs = ["a", "b", "c"]
        outputs = ["x", "y"]

        tree = _converge_op_xml(name="merge", inputs=inputs,
                outputs=outputs)

        op = wfxml.ConvergeOperation(log_dir="/tmp", xml=tree)
        self.assertEqual("merge", op.name)
        self.assertEqual(inputs, op.input_properties)
        self.assertEqual(outputs, op.output_properties)


class TestXmlAdapter(TestCase):
    def setUp(self):
        self.builder = nb.NetBuilder()

    def test_simple(self):
        xml = E.operation({"name": "pby_test", "parallelBy": "file"},
                E.operationtype(
                    {"typeClass": _wf_command_class, "commandClass": "X"}))

        model = wfxml.ModelOperation(xml)
        self.assertEqual("pby_test", model.name)
        self.assertEqual(3, len(model.operations))
        expected_names = ["input connector", "output connector", "pby_test"]
        self.assertEqual(expected_names, [x.name for x in model.operations])

        self.assertEqual(set([2]), model.edges[0])
        self.assertNotIn(1, model.edges)
        self.assertEqual(set([1]), model.edges[2])

        self.assertNotIn(0, model.rev_edges)
        self.assertEqual(set([2]), model.rev_edges[1])
        self.assertEqual(set([0]), model.rev_edges[2])


    def test_serial(self):
        xml = E.workflow({"name": "test"},
                _link_xml("input connector", "input", "job_1", "param"),
                _link_xml("job_1", "result", "job_2", "param"),
                _link_xml("job_2", "result", "job_3", "param"),
                _link_xml("job_3", "result", "output connector", "output"),
                E.operationtype({"typeClass": _wf_model_class},
                        E.inputproperty("input"),
                        E.outputproperty("output")),
                *[_command_op_xml("job_%d" % x, "ClassX") for x in (1, 2, 3)])


        model = wfxml.ModelOperation(xml)
        self.assertEqual("test", model.name)

        self.assertEqual(5, len(model.operations))
        expected_names = [ "input connector", "output connector" ]
        expected_names.extend(("job_%d" %x for x in xrange(1,4)))
        self.assertEqual(expected_names, [x.name for x in model.operations])
        self.assertEqual(wfxml.InputConnector, type(model.operations[0]))
        self.assertEqual(wfxml.OutputConnector, type(model.operations[1]))
        self.assertEqual([wfxml.CommandOperation]*3,
                         [type(x) for x in model.operations[2:]])

        ops = model.operations
        self.assertEqual(set([ops[2]]), model.edges[ops[0]])
        self.assertNotIn(ops[1], model.edges)
        self.assertEqual(set([ops[3]]), model.edges[ops[2]])
        self.assertEqual(set([ops[4]]), model.edges[ops[3]])
        self.assertEqual(set([ops[1]]), model.edges[ops[4]])

        self.assertNotIn(0, model.rev_edges)
        self.assertEqual(set([ops[4]]), model.rev_edges[ops[1]])
        self.assertEqual(set([ops[0]]), model.rev_edges[ops[2]])
        self.assertEqual(set([ops[2]]), model.rev_edges[ops[3]])
        self.assertEqual(set([ops[3]]), model.rev_edges[ops[4]])

        net = model.net(self.builder)
        self.assertIsInstance(net, wfnets.GenomeModelNet)
        self.assertEqual(5, len(net.subnets))

    def test_create_self_cycle(self):
        xml = E.workflow({"name": "test"},
                _command_op_xml("x", "x"),
                _link_xml("x", "px", "x", "py"),
                E.operationtype({"typeClass": _wf_model_class})
                )

        self.assertRaises(RuntimeError, wfxml.ModelOperation, xml)

    def test_missing_optype(self):
        xml = E.workflow({"name": "test"}, E.operation({"name": "bad"}),
                E.operationtype({"typeClass": _wf_model_class}))
        self.assertRaises(ValueError, wfxml.ModelOperation, xml)

    def test_unknown_optype(self):
        xml = E.workflow({"name": "test"},
                E.operationtype({"typeClass": _wf_model_class}),
                E.operation({"name": "bad"},
                    E.operationtype({"typeClass": "unknown"})))
        self.assertRaises(ValueError, wfxml.ModelOperation, xml)

    def test_nested_models(self):
        xml = E.workflow({"name": "test"},
                _link_xml("input connector", "x", "nested", "y"),
                _link_xml("nested", "result", "output connector", "result"),
                E.operationtype({"typeClass": _wf_model_class}),
                E.operation({"name": "nested"},
                    E.operationtype({"typeClass": _wf_model_class}),
                    _command_op_xml("nested", "ClassX")))

        model = wfxml.ModelOperation(xml)
        local_net = model.net(self.builder)
        self.builder.graph().draw("x.ps", prog="dot")


if __name__ == "__main__":
    main()
