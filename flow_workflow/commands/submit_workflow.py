from flow import exit_codes
from flow import petri
from flow.commands.base import CommandBase
from flow.configuration.inject.broker import BrokerConfiguration
from flow.configuration.inject.redis_conf import RedisConfiguration
from flow.service_locator import ServiceLocator
from flow.petri.netbase import PlaceEntryObservedMessage
from flow.handler import Handler
from flow_workflow import nets
from lxml import etree
from injector import inject
from twisted.internet import defer

import flow.interfaces
import flow.petri.netbuilder as nb
import flow_workflow.xmladapter as wfxml
import json
import os
import sys
import uuid


@inject(storage=flow.interfaces.IStorage,
        broker=flow.interfaces.IBroker,
        service_locator=ServiceLocator)
class SubmitWorkflowCommand(CommandBase):
    injector_modules = [
            BrokerConfiguration,
            RedisConfiguration,
    ]

    @staticmethod
    def annotate_parser(parser):
        parser.add_argument('--xml', '-x', help="Workflow definition xml file")
        parser.add_argument('--plot', '-p',
                help='Save an image of the net. The type is determined by the'
                'extension provided (e.g., x.ps, x.png, ...)')
        parser.add_argument('--block', default=False, action='store_true',
                help="If set, block until the workflow is complete")
        parser.add_argument('--inputs-file', '-i',
                help="File containing initial inputs (json format)")
        parser.add_argument('--resource-file', '-r', default=None,
                help='File mapping operation names to resource requests '
                '(json format)')
        parser.add_argument('--outputs-file', '-o',
                help="File to write final outputs to (json format) NOTE: implies --block")
        parser.add_argument('--email', '-e',
                help="If set, send notification emails to the given address")
        parser.add_argument('--no-submit', '-n', default=False,
                action='store_true',
                help="Create, but do not submit this workflow")
        parser.add_argument('--plan-id', '-P', type=int,
                help="The workflow plan id")


    def _create_initial_token(self, inputs_file):
        inputs = {}
        if inputs_file:
            inputs = json.load(open(inputs_file))
        token_data = {"outputs": inputs}
        return petri.Token.create(self.storage, data=token_data, data_type="output")

    def __call__(self, parsed_arguments):
        builder = nb.NetBuilder()
        parsed_xml = etree.XML(open(parsed_arguments.xml).read())
        if parsed_arguments.resource_file:
            resources = json.load(open(parsed_arguments.resource_file))
        else:
            resources = {}

        local_net = wfxml.parse_workflow_xml(parsed_xml, resources, builder,
                parsed_arguments.plan_id)

        if parsed_arguments.plot:
            graph = builder.graph(subnets=True)
            graph.draw(parsed_arguments.plot, prog="dot")
            print("Image saved to %s" % parsed_arguments.plot)

        stored_net = builder.store(self.storage)
        stored_net.capture_environment()

        lsf_project = 'flow %s' % stored_net.key
        print("Setting LSF project to %s" % lsf_project)
        stored_net.set_constant('lsf_project', lsf_project)

        if parsed_arguments.email:
            stored_net.set_constant("mail_user", parsed_arguments.email)

        token = self._create_initial_token(parsed_arguments.inputs_file)
        print("Resources: %r" % resources)
        print("Net key: %s" % stored_net.key)
        print("Initial token key: %s" % token.key)
        print("Initial inputs: %r" % token.data.value)

        if not parsed_arguments.no_submit:
            should_block = (parsed_arguments.block or
                    parsed_arguments.outputs_file is not None)
            if should_block:
                queue_name = self.add_done_place_observers(stored_net)
                handler = CompletedMessageHandler(self.broker, queue_name)
                self.broker.register_handler(handler)
            else:
                queue_name = None

            self.broker.add_ready_callback(self.once_connected, stored_net.key,
                    token.key, queue_name)
            self.broker.connect_and_listen()

            if parsed_arguments.outputs_file:
                outputs = nets.get_workflow_outputs(stored_net)
                json.dump(outputs, open(parsed_arguments.outputs_file, 'w'))


    @defer.inlineCallbacks
    def once_connected(self, stored_net_key, token_key, queue_name=None):
        if queue_name is not None:
            channel = self.broker.channel
            yield channel.queue_declare(queue=queue_name, durable=False,
                    auto_delete=False, exclusive=True)

        orchestrator = self.service_locator['orchestrator']
        yield orchestrator.set_token(net_key=stored_net_key,
                place_idx=0, token_key=token_key)

        if queue_name is None:
            self.broker.stop()

    def add_done_place_observers(self, stored_net):
        queue_name = generate_queue_name()

        success_place = stored_net.place(1)
        failure_place = stored_net.place(2)

        success_place.add_observer(exchange='', routing_key=queue_name,
                body='success')
        failure_place.add_observer(exchange='', routing_key=queue_name,
                body='failure')

        return queue_name

def generate_queue_name():
    return 'submit_flow_block_%s' % uuid.uuid4().hex

class CompletedMessageHandler(Handler):
    message_class = PlaceEntryObservedMessage
    def __init__(self, broker, queue_name):
        self.broker = broker
        self.queue_name = queue_name

    def _handle_message(self, message):
        self.broker.stop()

        status = message.body
        sys.stderr.write(
                'Submitted flow completed with result: %s\n' % status)
        if status != 'success':
            os._exit(exit_codes.EXECUTE_FAILURE)


