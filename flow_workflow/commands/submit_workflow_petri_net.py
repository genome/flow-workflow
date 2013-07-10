#from flow.commands.base import CommandBase
#from flow.exit_codes import EXECUTE_FAILURE
#from flow.handler import Handler
#from flow.service_locator import ServiceLocator
#from flow.util.exit import exit_process
#from flow_workflow.xml_adapater import parse_workflow_xml
#from injector import inject
#from lxml import etree
#from twisted.internet import defer
#from flow.configuration.inject.redis_conf import RedisConfiguration
#from flow.configuration.inject.broker import BrokerConfiguration
#
#import flow.interfaces
#import json
#import sys
#
#@inject(storage=flow.interfaces.IStorage,
#        broker=flow.interfaces.IBroker,
#        service_locator=ServiceLocator)
#class SubmitWorkflowCommand(CommandBase):
#    injector_modules = [
#            BrokerConfiguration,
#            RedisConfiguration,
#    ]
#
#    @staticmethod
#    def annotate_parser(parser):
#        parser.add_argument('--xml', '-x', help="Workflow definition xml file")
#        parser.add_argument('--block', default=False, action='store_true',
#                help="If set, block until the workflow is complete")
#        parser.add_argument('--inputs-file', '-i',
#                help="File containing initial inputs (json format)")
#        parser.add_argument('--resource-file', '-r', default=None,
#                help='File mapping operation names to resource requests '
#                '(json format)')
#        parser.add_argument('--outputs-file', '-o',
#                help="File to write final outputs to (json format) "
#                "NOTE: implies --block")
#        parser.add_argument('--email', '-e',
#                help="If set, send notification emails to the given address")
#        parser.add_argument('--plan-id', '-P', type=int,
#                help="The workflow plan id")
#        parser.add_argument('--project-name', '-N',
#                help="The project name to use for submitted jobs")
#
#    def _execute(self, parsed_arguments):
#        parsed_xml = self._parse_xml(parsed_arguments.xml)
#        resources = self._determine_resources(parsed_arguments.resource_file)
#        future_net, variables, constants = parse_workflow_xml(parsed_xml,
#                resources=resources,
#                plan_id=parsed_arguments.plan_id,
#                project_name=parsed_arguments.project_name,
#                email=parsed_arguments.email)
#
#        should_block = (parsed_arguments.block or
#            parsed_arguments.outputs_file is not None)
#        if should_block:
#            queue_name = self._add_done_transition_observers(future_net)
#        else:
#            queue_name = None
#
#        builder = Builder(connection=self.storage)
#        builder.store(future_net, variables, constants)
#        # TODO capture environment
#
#        # XXX store initial inputs
#
#        exit_deferred = defer.Deferred()
#        self._start_net(stored_net, queue_name,
#                parsed_arguments.outputs_file,
#                exit_deferred)
#        return exit_deferred
#
#    @defer.inlineCallbacks
#    def _start_net(self, stored_net, queue_name, outputs_file,
#            exit_deferred):
#        if queue_name is not None:
#            channel = self.broker.channel
#            yield channel.queue_declare(queue=queue_name,
#                    durable=False, auto_delete=False, exclusive=True)
#
#            handler = SubmitWorkflowMessageHandler(broker=self.broker,
#                    queue_name=queue_name, stored_net=stored_net,
#                    outputs_file=outputs_file, exit_deferred=exit_deferred)
#            yield self.broker.register_handler(handler)
#
#        orchestrator = self.service_locator['orchestrator']
#        yield orchestrator.create_token(stored_net.key, place_idx=0)
#
#        if queue_name is None:
#            exit_deferred.callback(None)
#
#    def _add_done_transition_observers(self, future_net):
#        # TODO
#        return queue_name
#
#    def _parse_xml(filename):
#        with open(filename) as xml_infile:
#            result = etree.XML(xml_infile.read())
#        return result
#
#    def _determine_resources(filename):
#        result = None
#        if filename is not None:
#            with open(filename) as resources_infile:
#                result = json.load(resources_infile)
#        return result
#
#
#class SubmitWorkflowMessageHandler(Handler):
#    message_class = PlaceEntryObservedMessage
#    def __init__(self, broker, queue_name, stored_net,
#                outputs_file, exit_deferred):
#        self.broker = broker
#        self.queue_name = queue_name
#        self.stored_net = stored_net
#        self.outputs_file = outputs_file
#        self.exit_deferred = exit_deferred
#
#    def _handle_message(self, message):
#        if self.outputs_file is not None:
#            outputs = nets.get_workflow_outputs(self.stored_net)
#            json.dump(outputs, open(self.outputs_file, 'w'))
#
#        status = message.body
#        sys.stderr.write(
#                'Submitted flow completed with result: %s\n' % status)
#        if status != 'success':
#            exit_process(EXECUTE_FAILURE)
#        else:
#            self.exit_deferred.callback(None)
#        return defer.succeed(None)
