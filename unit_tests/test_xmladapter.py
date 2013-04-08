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
    type_attr['typeClass'] = _wf_event_class

    return _op_xml(op_attr, type_attr)


def _converge_op_xml(name, inputs, outputs):
    subelts = ([E.inputproperty(x) for x in inputs] +
            [E.outputproperty(x) for x in outputs])

    return E.operation({"name": name},
            E.operationtype({"typeClass": _wf_converge_class}, *subelts))


class TestWorkflowEntity(TestCase):
    def test_abstract_net(self):
        builder = nb.NetBuilder()
        entity = wfxml.WorkflowEntity(operation_id=1)
        self.assertRaises(NotImplementedError, entity.net, builder)


class TestWorkflowOperations(TestCase):
    def setUp(self):
        self.factory = wfxml.WorkflowEntityFactory.get_instance()
        self.factory.next_operation_id = 0
        self.builder = nb.NetBuilder()
        self.resources = {}

    def test_no_operationtype_tag(self):
        tree = E.operation({"name": "badguy"})
        self.assertRaises(ValueError,
                wfxml.WorkflowOperation, operation_id=1, xml=tree,
                log_dir="/tmp", parent=None)

    def test_multiple_operationtype_tags(self):
        type_attr = {"commandClass": "A", "typeClass": _wf_command_class}
        tree = E.operation({"name": "badguy"},
                E.operationtype(type_attr),
                E.operationtype(type_attr)
        )
        self.assertRaises(ValueError,
                wfxml.WorkflowOperation, operation_id=1, xml=tree,
                log_dir="/tmp", parent=None)

    def test_command(self):
        tree = _command_op_xml(name="op nums 1/2", perl_class="ClassX",
                op_attr={"a": "b"}, type_attr={"x": "y"})

        op = self.factory.create_from_xml(tree, log_dir="/tmp", parent=None,
                resources=self.resources)

        self.assertEqual(0, op.operation_id)
        self.assertEqual("op nums 1/2", op.name)
        self.assertEqual("b", op._operation_attributes["a"])
        self.assertEqual("y", op._type_attributes["x"])

        self.assertEqual("/tmp", op.log_dir)
        self.assertRegexpMatches(op.stdout_log_file,
                "/tmp/op_nums_1_2\.[0-9]+\.out")
        self.assertRegexpMatches(op.stderr_log_file,
                "/tmp/op_nums_1_2\.[0-9]+\.err")

        self.assertEqual("ClassX", op.perl_class)
        self.assertEqual("", op.parallel_by)

        input_conns = {0: {"x": "y"}}

        net = op.net(self.builder, input_connections=input_conns)
        self.assertIsInstance(net, wfnets.GenomePerlActionNet)
        self.assertEqual("command", net.action_type)
        self.assertEqual("ClassX", net.action_id)

        self.assertIsInstance(net.shortcut, exnets.LocalCommandNet)
        shortcut_transition = net.shortcut.dispatch
        action = shortcut_transition.action
        self.assertIsInstance(action, nb.ActionSpec)
        self.assertEqual(wfnets.GenomeShortcutAction, action.cls)

        expected_args = {
                "action_type": "command",
                "method": "shortcut",
                "action_id": "ClassX",
                "with_outputs": True,
                "input_connections": input_conns,
        }
        self.assertDictContainsSubset(expected_args, action.args)

        expected_args["method"] = "execute"
        self.assertIsInstance(net.execute, exnets.LSFCommandNet)
        execute_transition = net.execute.dispatch
        action = execute_transition.action
        self.assertIsInstance(action, nb.ActionSpec)
        self.assertEqual(wfnets.GenomeExecuteAction, action.cls)

    def test_parallel_by_command(self):
        tree = _command_op_xml(name="pby", perl_class="X",
                op_attr={"parallelBy": "input_file"})

        op = self.factory.create_from_xml(tree, log_dir="/tmp", parent=None,
                resources=self.resources)

        self.assertEqual("input_file", op.parallel_by)

    def test_event(self):
        tree = _event_op_xml(name="evt", event_id="123")

        op = self.factory.create_from_xml(xml=tree, log_dir="/tmp", parent=None,
                resources=self.resources)

        self.assertEqual("evt", op.name)
        self.assertEqual("123", op.event_id)

    def test_converge_exceptions(self):
        tree = _converge_op_xml(name="merge", inputs=[], outputs=["x"])
        self.assertRaises(ValueError,
                wfxml.ConvergeOperation, operation_id=1, xml=tree,
                log_dir="/tmp", parent=None, resources=self.resources)

        tree = _converge_op_xml(name="merge", inputs=["x"], outputs=[])
        self.assertRaises(ValueError,
                wfxml.ConvergeOperation, operation_id=1, xml=tree,
                log_dir="/tmp", parent=None, resources=self.resources)


    def test_converge(self):
        inputs = ["a", "b", "c"]
        outputs = ["x", "y"]

        tree = _converge_op_xml(name="merge", inputs=inputs,
                outputs=outputs)

        op = self.factory.create_from_xml(tree, log_dir="/tmp", parent=None,
                resources=self.resources)

        self.assertEqual("merge", op.name)
        self.assertEqual(inputs, op.input_properties)
        self.assertEqual(outputs, op.output_properties)


class TestXmlAdapter(TestCase):
    def setUp(self):
        self.builder = nb.NetBuilder()
        self.resources = {}
        self.factory = wfxml.WorkflowEntityFactory.get_instance()

        self.serial_xml = E.workflow({"name": "test"},
                _link_xml("input connector", "input", "job_1", "param"),
                _link_xml("job_1", "result", "job_2", "param"),
                _link_xml("job_2", "result", "job_3", "param"),
                _link_xml("job_3", "result", "output connector", "output"),
                E.operationtype({"typeClass": _wf_model_class},
                        E.inputproperty("input"),
                        E.outputproperty("output")),
                *[_command_op_xml("job_%d" % x, "ClassX") for x in (1, 2, 3)])

    def test_simple(self):
        xml = E.operation({"name": "pby_test", "parallelBy": "file"},
                E.operationtype(
                    {"typeClass": _wf_command_class, "commandClass": "X"}))

        model = self.factory.create("Workflow::OperationType::Model",
                xml=xml, log_dir="/tmp", parent=None, resources=self.resources)

        self.assertEqual("pby_test", model.name)
        self.assertEqual(3, len(model.operations))
        expected_names = ["input connector", "output connector", "pby_test"]
        self.assertEqual(expected_names, [x.name for x in model.operations])

        self.assertEqual(set([model.operations[2]]),
                model.edges[model.operations[0]])
        self.assertNotIn(1, model.edges)
        self.assertEqual(set([model.operations[1]]),
                model.edges[model.operations[2]])

        self.assertNotIn(0, model.rev_edges)
        self.assertEqual(set([model.operations[2]]),
                model.rev_edges[model.operations[1]])
        self.assertEqual(set([model.operations[0]]),
                model.rev_edges[model.operations[2]])

        net = model.net(self.builder)


    def test_serial(self):
        model = self.factory.create_from_xml(self.serial_xml, log_dir="/tmp",
                parent=None, resources=self.resources)

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

        self.assertRaises(RuntimeError, self.factory.create,
                "Workflow::OperationType::Model", xml=xml, log_dir="/tmp",
                resources=self.resources, parent=None)

    def test_missing_optype(self):
        xml = E.workflow({"name": "test"}, E.operation({"name": "bad"}),
                E.operationtype({"typeClass": _wf_model_class}))
        self.assertRaises(ValueError, self.factory.create,
                "Workflow::OperationType::Model", xml=xml, log_dir="/tmp",
                resources=self.resources, parent=None)

    def test_unknown_optype(self):
        xml = E.workflow({"name": "test"},
                E.operationtype({"typeClass": _wf_model_class}),
                E.operation({"name": "bad"},
                    E.operationtype({"typeClass": "unknown"})))
        self.assertRaises(ValueError, self.factory.create,
                "Workflow::OperationType::Model", xml=xml, log_dir="/tmp",
                resources=self.resources)

    def test_nested_models(self):
        xml = E.workflow({"name": "test"},
                _link_xml("input connector", "x", "nested", "y"),
                _link_xml("nested", "result", "output connector", "result"),
                E.operationtype({"typeClass": _wf_model_class}),
                E.operation({"name": "nested"},
                    E.operationtype({"typeClass": _wf_model_class}),
                    _command_op_xml("nested", "ClassX")))

        model = self.factory.create_from_xml(xml=xml, log_dir="/tmp",
                resources=self.resources)

        net = model.net(self.builder)

        self.assertIsInstance(net, wfnets.GenomeModelNet)
        self.assertEqual(3, len(net.subnets))
        subnet = net.subnets[model._first_operation_idx]
        self.assertIsInstance(subnet, wfnets.GenomeModelNet)
        self.assertEqual(3, len(subnet.subnets))

        cmd = subnet.subnets[model._first_operation_idx]
        self.assertIsInstance(cmd, wfnets.GenomePerlActionNet)

    def test_converge(self):
        xml = E.workflow({"name": "test converge"},
                _link_xml("input connector", "a", "A", "a"),
                _link_xml("input connector", "b", "B", "b"),
                _link_xml("A", "a", "C", "a"),
                _link_xml("B", "B", "C", "b"),
                E.operationtype({"typeClass": _wf_model_class}),
                _command_op_xml("A", "ClassA"),
                _command_op_xml("B", "ClassB"),
                _converge_op_xml("C", inputs=["a", "b"], outputs=["c", "d"]))

        model = self.factory.create_from_xml( xml, log_dir="/tmp",
                resources=self.resources)

        net = model.net(self.builder)
        self.assertIsInstance(net, wfnets.GenomeModelNet)
        self.assertEqual(5, len(net.subnets))
        self.assertIsInstance(net.subnets[2], wfnets.GenomePerlActionNet)
        self.assertIsInstance(net.subnets[3], wfnets.GenomePerlActionNet)
        self.assertIsInstance(net.subnets[4], wfnets.GenomeConvergeNet)

    def test_event(self):
        xml = E.workflow({"name": "test event"},
                _link_xml("input connector", "a", "A", "a"),
                _link_xml("input connector", "b", "B", "b"),
                _link_xml("A", "a", "C", "a"),
                _link_xml("B", "b", "C", "b"),
                _link_xml("C", "c", "output connector", "c"),
                E.operationtype({"typeClass": _wf_model_class}),
                _event_op_xml("A", event_id="100"),
                _event_op_xml("B", event_id="200"),
                _event_op_xml("C", event_id="300"))

        model = self.factory.create_from_xml(xml=xml, log_dir="/tmp",
                resources=self.resources)

        net = model.net(self.builder)
        self.assertIsInstance(net, wfnets.GenomeModelNet)
        self.assertEqual(5, len(net.subnets))

        for subnet in net.subnets[model._first_operation_idx:]:
            self.assertIsInstance(subnet, wfnets.GenomePerlActionNet)
            self.assertEqual("event", subnet.action_type)

        self.assertEqual("100", net.subnets[2].action_id)
        self.assertEqual("200", net.subnets[3].action_id)
        self.assertEqual("300", net.subnets[4].action_id)

    def test_parse_workflow_xml(self):
        outer_net = wfxml.parse_workflow_xml(self.serial_xml, self.resources,
                self.builder, plan_id=1234)

        self.assertIsInstance(outer_net.start, nb.Place)
        self.assertIsInstance(outer_net.success, nb.Place)
        self.assertIsInstance(outer_net.failure, nb.Place)
        self.assertEqual(1, len(outer_net.subnets))

        net = outer_net.subnets[0]
        self.assertIsInstance(net, wfnets.GenomeModelNet)
        self.assertEqual(5, len(net.subnets))
        expected_names = [
                "input connector",
                "output connector",
                "job_1", "job_2", "job_3"
                ]

        names = [x.name for x in net.subnets]
        self.assertItemsEqual(expected_names, names)

    def test_parallel_by_model(self):
        xml = E.workflow({"name": "test parallel by"},
                _link_xml("input connector", "file", "operation", "file"),
                _link_xml("operation", "result", "output connector", "result"),
                E.operationtype({"typeClass": _wf_model_class}),
                _command_op_xml(name="operation", perl_class="X",
                        op_attr={"parallelBy": "input_file"}))

        net = wfxml.parse_workflow_xml(xml, self.resources, self.builder,
                plan_id=123)


if __name__ == "__main__":
    main()
