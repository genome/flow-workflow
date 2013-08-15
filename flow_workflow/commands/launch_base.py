from flow import exit_codes
from flow.commands.base import CommandBase
from flow.petri_net.builder import Builder
from flow.service_locator import ServiceLocator
from flow.util.exit import exit_process
from flow.exit_codes import EXECUTE_ERROR
from flow_workflow import factory
from flow_workflow.completion import MonitoringCompletionHandler
from flow_workflow.entities.workflow.adapters import WorkflowAdapter
from flow_workflow.future_operation import ForeignFutureOperation
from flow_workflow.future_operation import NullFutureOperation
from flow_workflow.historian.operation_data import OperationData
from flow_workflow.parallel_id import ParallelIdentifier
from lxml import etree
from twisted.internet import defer

import abc
import flow.interfaces
import injector
import json
import logging
import os
import pwd


LOG = logging.getLogger(__name__)


@injector.inject(storage=flow.interfaces.IStorage,
        broker=flow.interfaces.IBroker,
        service_locator=ServiceLocator,
        injector=injector.Injector)
class LaunchWorkflowCommandBase(CommandBase):
    def setup_completion_handler(self, net):
        declare_deferred = self.broker.declare_queue(net.key, durable=False,
                exclusive=True)
        done_deferred = defer.Deferred()
        declare_deferred.addCallback(self._register_completion_handler,
                done_deferred=done_deferred, net_key=net.key)
        return done_deferred

    def _register_completion_handler(self, _, done_deferred, net_key):
        self.completion_handler = MonitoringCompletionHandler(
                queue_name=net_key, done_deferred=done_deferred)
        self.broker.register_handler(self.completion_handler)

    @staticmethod
    def annotate_parser(parser):
        parser.add_argument('--xml', '-x', required=True,
                help="Workflow definition xml file")

        parser.add_argument('--inputs-file', '-i',
                help="File containing initial inputs (json format)")
        parser.add_argument('--resource-file', '-r', default=None,
                help='File mapping operation names to resource requests '
                     '(json format)')
        parser.add_argument('--outputs-file', '-o',
                help="File to write final outputs to (json format) "
                     "NOTE: implies --block")

        parser.add_argument('--plan-id', '-P', type=int,
                help="The workflow plan id")

        parser.add_argument('--block', action='store_true')

        # XXX Currently ignored
        parser.add_argument('--project-name', '-N',
                help="The project name to use for submitted jobs")

        parser.add_argument('--email', '-e',
                help="If set, send notification emails to the given address")

    @abc.abstractproperty
    def local_workflow(self):
        raise NotImplementedError()

    @abc.abstractmethod
    def setup_services(self, net):
        pass

    @abc.abstractmethod
    def wait_for_results(self, net, block):
        raise NotImplementedError()


    def _execute(self, parsed_arguments):
        workflow, net, start_place = self.construct_net(parsed_arguments.xml,
                parsed_arguments.inputs_file, parsed_arguments.resource_file)

        self.setup_services(net)

        if parsed_arguments.plan_id:
            net.set_constant('workflow_plan_id', parsed_arguments.plan_id)

        _execute_deferred = defer.Deferred()
        start_deferred = self.start_net(net, start_place)
        start_deferred.addCallback(self._on_net_started,
                parsed_arguments=parsed_arguments, workflow=workflow, net=net,
                _execute_deferred=_execute_deferred)
        start_deferred.addErrback(self._on_net_started_failed)
        return _execute_deferred

    def _on_net_started(self, _callback, parsed_arguments, workflow, net,
            _execute_deferred):
        results_deferred = self.wait_for_results(net=net,
                block=parsed_arguments.block)
        results_deferred.addCallback(self._on_results,
                parsed_arguments=parsed_arguments, workflow=workflow, net=net,
                _execute_deferred=_execute_deferred)
        results_deferred.addErrback(self._on_results_failed)
        return _callback

    def _on_results(self, block, parsed_arguments, workflow, net,
            _execute_deferred):
        LOG.debug('Workflow execution done, block = %s', block)

        if block:
            if self.complete():
                self.write_outputs(net, workflow.child_adapter.operation_id,
                        workflow.output_properties,
                        parsed_arguments.outputs_file)

            else:
                LOG.info('Workflow execution failed.')
                exit_process(exit_codes.EXECUTE_FAILURE)
        _execute_deferred.callback(None)
        return block

    def _on_results_failed(self, error):
        LOG.critical("Failed to get results\n%s",
                error.getTraceback())
        exit_process(EXECUTE_ERROR)

    def _on_net_started_failed(self, error):
        LOG.critical("Failed to start net\n%s",
                error.getTraceback())
        exit_process(EXECUTE_ERROR)

    def complete(self):
        return self.completion_handler.status == 'success'


    def start_net(self, net, start_place):
        cg = net.add_color_group(1)
        orchestrator = self.service_locator['orchestrator']
        return orchestrator.create_token(net.key, start_place, cg.begin, cg.idx)

    @property
    def operation_data(self):
        string = os.environ.get('FLOW_WORKFLOW_OPERATION_DATA')
        if string:
            return OperationData.loads(string)
        else:
            return False

    def construct_net(self, xml_filename, inputs_filename, resources_filename):
        xml = load_xml(xml_filename)
        inputs = load_inputs(inputs_filename)
        resources = load_resources(resources_filename)

        workflow = WorkflowAdapter(xml, inputs,
                local_workflow=self.local_workflow)

        future_net = workflow.future_net(resources)

        # XXX Update builder to use injector
        builder = Builder(self.storage)
        stored_net = builder.store(future_net, self.variables, self.constants)

        if self.operation_data:
            parent_future_op = ForeignFutureOperation(
                    operation_data=self.operation_data)
            stored_net.set_initial_color(self.operation_data.color)
        else:
            parent_future_op = NullFutureOperation()

        future_operations = workflow.future_operations(parent_future_op,
                input_connections=None, output_properties=None)

        for future_operation in future_operations:
            future_operation.save(stored_net)
        workflow.store_inputs(stored_net)

        start_place_index = builder.future_places[future_net.start_place]

        return workflow, stored_net, start_place_index

    def write_outputs(self, net, operation_id, output_properties, outputs_file):
        if outputs_file:
            op = factory.load_operation(net=net, operation_id=operation_id)
            outputs = op.load_outputs(parallel_id=ParallelIdentifier())

            with open(outputs_file, 'w') as f:
                json.dump(outputs, f)


    @property
    def variables(self):
        return {}

    @property
    def constants(self):
        return {
            'environment': os.environ.data,
            'group_id': os.getgid(),
            'user_id': os.getuid(),
            'user_name': self.user_name,
            'working_directory': os.getcwd(),
        }

    @property
    def user_name(self):
        return pwd.getpwuid(os.getuid())[0]


def load_xml(filename):
    with open(filename) as f:
        xml = etree.XML(f.read())

    return xml


def load_inputs(filename):
    inputs = {}
    if filename:
        with open(filename) as f:
            inputs = json.load(f)
    return inputs


def load_resources(filename):
    resources = {}
    if filename:
        with open(filename) as f:
            resources = json.load(f)
    return resources
