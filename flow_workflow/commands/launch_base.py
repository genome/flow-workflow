from flow import exit_codes
from flow.commands.base import CommandBase
from flow.petri_net.builder import Builder
from flow.service_locator import ServiceLocator
from flow.util.exit import exit_process
from flow_workflow import io
from flow_workflow.completion import MonitoringCompletionHandler
from flow_workflow.parallel_id import ParallelIdentifier
from flow_workflow.entities.workflow.adapter import Workflow
from lxml import etree

import abc
import flow.interfaces
import injector
import json
import os

import logging

LOG = logging.getLogger(__name__)

@injector.inject(storage=flow.interfaces.IStorage,
        broker=flow.interfaces.IBroker,
        service_locator=ServiceLocator,
        injector=injector.Injector)
class LaunchWorkflowCommandBase(CommandBase):
    def setup_completion_handler(self, net):
        self.broker.declare_queue(net.key, durable=False, exclusive=True)
        self.completion_handler = MonitoringCompletionHandler(
                queue_name=net.key)
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

        # XXX Currently ignored
        parser.add_argument('--email', '-e',
                help="If set, send notification emails to the given address")

        parser.add_argument('--plan-id', '-P', type=int,
                help="The workflow plan id")

        parser.add_argument('--project-name', '-N',
                help="The project name to use for submitted jobs")

        parser.add_argument('--block', action='store_true')


    @abc.abstractmethod
    def setup_services(self, net):
        pass


    def _execute(self, parsed_arguments):
        workflow, net, start_place = self.construct_net(parsed_arguments.xml,
                parsed_arguments.inputs_file, parsed_arguments.resource_file)

        self.setup_services(net)

        self.start_net(net, start_place)

        if self.complete():
            self.write_outputs(net, workflow.child_adapter.operation_id,
                    workflow.output_properties, parsed_arguments.outputs_file)
        else:
            LOG.info('Workflow execution failed.')
            exit_process(exit_codes.EXECUTE_FAILURE)


    def complete(self):
        return self.completion_handler.status == 'success'


    def start_net(self, net, start_place):
        cg = net.add_color_group(1)
        orchestrator = self.service_locator['orchestrator']
        orchestrator.create_token(net.key, start_place, cg.begin, cg.idx)

        self.broker.listen()

    def construct_net(self, xml_filename, inputs_filename, resources_filename):
        xml = load_xml(xml_filename)
        inputs = load_inputs(inputs_filename)
        resources = load_resources(resources_filename)

        workflow = Workflow(xml, inputs, resources,
                local_workflow=self.local_workflow)

        future_net = workflow.future_net

        # XXX Update builder to use injector
        builder = Builder(self.storage)
        stored_net = builder.store(future_net, self.variables, self.constants)
        workflow.store_inputs(stored_net)

        start_place_index = builder.future_places[future_net.start_place]

        return workflow, stored_net, start_place_index

    def write_outputs(self, net, operation_id, output_properties, outputs_file):
        if outputs_file:
            outputs = io.load_outputs(net=net, operation_id=operation_id,
                    property_names=output_properties,
                    parallel_id=ParallelIdentifier())
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
            'working_directory': os.getcwd(),
        }



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
