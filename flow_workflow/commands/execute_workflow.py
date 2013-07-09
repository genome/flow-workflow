from flow import exit_codes
from flow.commands.base import CommandBase
from flow.configuration.inject.local_broker import BrokerConfiguration
from flow.configuration.inject.redis_conf import RedisConfiguration
from flow.configuration.inject.service_locator import ServiceLocatorConfiguration
from flow.orchestrator.handlers import PetriCreateTokenHandler
from flow.orchestrator.handlers import PetriNotifyPlaceHandler
from flow.orchestrator.handlers import PetriNotifyTransitionHandler
from flow.petri_net.builder import Builder
from flow.service_locator import ServiceLocator
from flow.shell_command.fork.handler import ForkShellCommandMessageHandler
from flow_workflow.workflow import Workflow
from lxml import etree

import flow.interfaces
import injector
import json
import os


@injector.inject(storage=flow.interfaces.IStorage,
        broker=flow.interfaces.IBroker,
        service_locator=ServiceLocator,
        injector=injector.Injector)
class ExecuteWorkflowCommand(CommandBase):
    injector_modules = [
            BrokerConfiguration,
            RedisConfiguration,
            ServiceLocatorConfiguration,
    ]

    def setup_services(self):
        self.setup_shell_command_handlers()
        self.setup_orchestrator_handlers()

    def setup_shell_command_handlers(self):
        self.broker.register_handler(
                self.injector.get(ForkShellCommandMessageHandler))

    def setup_orchestrator_handlers(self):
        self.broker.register_handler(
                self.injector.get(PetriCreateTokenHandler))
        self.broker.register_handler(
                self.injector.get(PetriNotifyPlaceHandler))
        self.broker.register_handler(
                self.injector.get(PetriNotifyTransitionHandler))

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
        parser.add_argument('--project-name', '-N',
                help="The project name to use for submitted jobs")


        parser.add_argument('--email', '-e',
                help="If set, send notification emails to the given address")


    def _execute(self, parsed_arguments):
        net, start_place = self.construct_net(parsed_arguments.xml,
                parsed_arguments.inputs_file, parsed_arguments.resource_file)

        self.setup_services()

        self.start_net(net, start_place)

    def start_net(self, net, start_place):
        cg = net.add_color_group(1)
        orchestrator = self.service_locator['orchestrator']
        orchestrator.create_token(net.key, start_place, cg.begin, cg.idx)

        self.broker.listen()

    def construct_net(self, xml_filename, inputs_filename, resources_filename):
        xml = load_xml(xml_filename)
        inputs = load_inputs(inputs_filename)
        resources = load_resources(resources_filename)

        workflow = Workflow(xml, inputs, resources)

        future_net = workflow.future_net

        # XXX Update builder to use injector
        builder = Builder(self.storage)
        stored_net = builder.store(future_net, self.variables, self.constants)
        workflow.store_inputs(stored_net)

        start_place_index = builder.future_places[future_net.start_place]

        return stored_net, start_place_index

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
