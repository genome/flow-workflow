from flow.commands.base import CommandBase
import flow.petri.netbuilder as nb
import flow.petri.safenet as sn
import flow_workflow.xmladapter as wfxml

from lxml import etree
import json
import os
import sys
import uuid

class SubmitWorkflowCommand(CommandBase):
    def __init__(self, broker=None, storage=None, orchestrator=None):
        self.broker = broker
        self.storage = storage
        self.orchestrator = orchestrator
        self.orchestrator.broker = broker

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
        parser.add_argument('--outputs-file', '-o',
                help="File to write final outputs to (json format)")
        parser.add_argument('--email', '-e',
                help="If set, send notification emails to the given address")
        parser.add_argument('--no-submit', '-n', default=False,
                action='store_true',
                help="Create, but do not submit this workflow")


    def _create_initial_token(self, inputs_file):
        inputs = {}
        if inputs_file:
            inputs = json.load(open(inputs_file))
        return sn.Token.create(self.storage, data=inputs, data_type="output")

    def __call__(self, parsed_arguments):
        builder = nb.NetBuilder()
        parsed_xml = etree.XML(open(parsed_arguments.xml).read())
        local_net = wfxml.parse_workflow_xml(parsed_xml, builder)

        if parsed_arguments.plot:
            graph = builder.graph(subnets=True)
            graph.draw(parsed_arguments.plot, prog="dot")
            print("Image saved to %s" % parsed_arguments.plot)

        stored_net = builder.store(self.storage)
        stored_net.capture_environment()

        if parsed_arguments.email:
            stored_net.set_constant("mail_user", parsed_arguments.email)

        if parsed_arguments.block:
            queue_name = self.add_done_place_observers(stored_net)

        token = self._create_initial_token(parsed_arguments.inputs_file)
        print("Net key: %s" % stored_net.key)
        print("Initial token key: %s" % token.key)
        print("Initial inputs: %r" % token.data.value)

        if not parsed_arguments.no_submit:
            self.broker.connect()
            self.orchestrator.set_token(net_key=stored_net.key, place_idx=0,
                    token_key=token.key)

            if parsed_arguments.block:
                self.broker.create_temporary_queue(queue_name)
                message = self.broker.raw_get(queue_name)
                sys.stderr.write(
                        'Submitted flow completed with result: %s\n' % message)
                if message != 'success':
                    os._exit(1)

            self.broker.disconnect()

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
