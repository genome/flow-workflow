from flow import exit_codes
from flow import petri
from flow.commands.base import CommandBase
from flow.configuration.inject.broker import BrokerConfiguration
from flow.configuration.inject.redis_conf import RedisConfiguration
from flow.service_locator import ServiceLocator
from flow.orchestrator.messages import PlaceEntryObservedMessage
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
        parser.add_argument('--project-name', '-N',
                help="The project name to use for submitted jobs")


    def _create_initial_token_data(self, inputs_file):
        inputs = {}
        if inputs_file:
            inputs = json.load(open(inputs_file))
        return {"outputs": inputs}

    def _execute(self, parsed_arguments):
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

        if parsed_arguments.project_name:
            lsf_project = parsed_arguments.project_name
        else:
            lsf_project = 'flow %s' % stored_net.key

        print("Setting LSF project to %s" % lsf_project)
        stored_net.set_constant('lsf_project', lsf_project)

        if parsed_arguments.email:
            stored_net.set_constant("mail_user", parsed_arguments.email)

        token_data = self._create_initial_token_data(
                parsed_arguments.inputs_file)
        print("Resources: %r" % resources)
        print("Net key: %s" % stored_net.key)

        exit_deferred = defer.Deferred()
        if not parsed_arguments.no_submit:
            should_block = (parsed_arguments.block or
                    parsed_arguments.outputs_file is not None)
            if should_block:
                queue_name = self.add_done_place_observers(stored_net)
            else:
                queue_name = None

            deferred = self.broker.connect()
            deferred.addCallback(self.once_connected, stored_net=stored_net,
                    token_data=token_data, queue_name=queue_name,
                    outputs_file=parsed_arguments.outputs_file,
                    exit_deferred=exit_deferred)
            return exit_deferred
        else:
            exit_deferred.callback(None)
            return exit_deferred

    @defer.inlineCallbacks
    def once_connected(self, _, stored_net, token_data, queue_name, outputs_file,
                exit_deferred):
        if queue_name is not None:
            channel = self.broker.channel
            yield channel.queue_declare(queue=queue_name, durable=False,
                    auto_delete=False, exclusive=True)

            handler = SubmitWorkflowMessageHandler(broker=self.broker,
                    queue_name=queue_name, stored_net=stored_net,
                    outputs_file=outputs_file, exit_deferred=exit_deferred)
            self.broker.register_handler(handler)
            self.broker.start_handler(handler)

        orchestrator = self.service_locator['orchestrator']
        yield orchestrator.create_token(stored_net.key, place_idx=0,
                   data=token_data, data_type="output")

        if queue_name is None:
            exit_deferred.callback(None)

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


class SubmitWorkflowMessageHandler(Handler):
    message_class = PlaceEntryObservedMessage
    def __init__(self, broker, queue_name, stored_net,
                outputs_file, exit_deferred):
        self.broker = broker
        self.queue_name = queue_name
        self.stored_net = stored_net
        self.outputs_file = outputs_file
        self.exit_deferred = exit_deferred

    def _handle_message(self, message):
        if self.outputs_file is not None:
            outputs = nets.get_workflow_outputs(self.stored_net)
            json.dump(outputs, open(self.outputs_file, 'w'))

        status = message.body
        sys.stderr.write(
                'Submitted flow completed with result: %s\n' % status)
        if status != 'success':
            os._exit(exit_codes.EXECUTE_FAILURE)
        else:
            self.exit_deferred.callback(None)
        return defer.succeed(None)
