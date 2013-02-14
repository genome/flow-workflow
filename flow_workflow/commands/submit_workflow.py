from flow.commands.base import CommandBase
import flow.petri.netbuilder as nb
import flow.petri.safenet as sn
import flow_workflow.xmladapter as wfxml

from lxml import etree
import json
import os

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


    def _create_local_net(self, xml_file, builder):
        parsed_xml = etree.XML(open(xml_file).read())
        model = wfxml.convert_workflow_xml(parsed_xml)
        return model.net(builder)

    def _create_initial_token(self, inputs_file):
        inputs = {}
        if inputs_file:
            inputs = json.load(open(inputs_file))
        return sn.Token.create(self.storage, data=inputs, data_type="output")

    def _add_notifications(self, local_net):
        p_success = local_net.add_place("success notification sent")
        p_failure = local_net.add_place("failure notification sent")

        t_success = local_net.bridge_places(local_net.success, p_success)
        t_failure = local_net.bridge_places(local_net.failure, p_failure)

    def __call__(self, parsed_arguments):
        builder = nb.NetBuilder("workflow")
        local_net = self._create_local_net(parsed_arguments.xml, builder)

        if parsed_arguments.block:
            self._add_notifications(local_net)

        if parsed_arguments.plot:
            graph = builder.graph(subnets=True)
            graph.draw(parsed_arguments.plot, prog="dot")
            print "Image saved to %s" % parsed_arguments.plot

        stored_net = builder.store(self.storage)
        stored_net.capture_environment()
        if parsed_arguments.email:
            stored_net.set_constant("mail_user", parsed_arguments.email)



        token = self._create_initial_token(parsed_arguments.inputs_file)
        print "Net key: %s" % stored_net.key
        print "Initial token key: %s" % token.key
        print "Initial inputs: %r" % token.data.value

        self.broker.connect()
        self.orchestrator.set_token(net_key=stored_net.key, place_idx=0,
                token_key=token.key)
