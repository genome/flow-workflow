from flow import redisom as rom
from flow.commands.base import CommandBase
from flow.configuration.inject.redis_conf import RedisConfiguration
from flow.configuration.settings.injector import setting
from flow.util.logannotator import LogAnnotator
from flow_workflow import io
from flow_workflow.parallel_id import ParallelIdentifier
from injector import inject
from tempfile import NamedTemporaryFile
from twisted.internet import defer

import copy
import flow.interfaces
import json
import logging
import socket


LOG = logging.getLogger(__name__)


def _build_command_cmdline(method, action_id, inputs_file, outputs_file):
    return ["command", method, action_id, inputs_file.name, outputs_file.name]


def _build_event_cmdline(method, action_id, inputs_file, outputs_file):
    return ["event", method, action_id, outputs_file.name]

CMDLINE_BUILDERS = {
        'command':_build_command_cmdline,
        'event':_build_event_cmdline,
        }

@inject(perl_wrapper=setting('workflow.perl_wrapper'),
        storage=flow.interfaces.IStorage)
class WorkflowWrapperCommand(CommandBase):
    injector_modules = [RedisConfiguration]

    @staticmethod
    def annotate_parser(parser):
        parser.add_argument("--method", required=True,
                help="shortcut or execute")
        parser.add_argument('--action-type', required=True,
                help='event or command')
        parser.add_argument('--action-id', required=True,
                help='event_id or perl_class')

        parser.add_argument('--net-key', required=True,
                help='used to look up inputs')
        parser.add_argument('--operation-id', type=int, required=True,
                help='used to look up inputs')
        parser.add_argument('--input-connections', required=True,
                help='used to look up inputs')

        parser.add_argument('--parallel-id', default=None,
                help='used to look up inputs')


    @defer.inlineCallbacks
    def _execute(self, parsed_arguments):
        try:
            net = rom.get_object(self.storage, parsed_arguments.net_key)

            parallel_id = parse_parallel_id(parsed_arguments.parallel_id)

            with NamedTemporaryFile() as inputs_file:
                with NamedTemporaryFile() as outputs_file:
                    write_inputs(inputs_file, net=net, parallel_id=parallel_id,
                            input_connections=parsed_arguments.input_connections)

                    cmdline = copy.copy(self.perl_wrapper)
                    cmdline_builder = CMDLINE_BUILDERS[parsed_arguments.action_type]
                    cmdline.extend(cmdline_builder(parsed_arguments.method,
                        parsed_arguments.action_id, inputs_file, outputs_file))


                    LOG.debug('Executing (%s): %s', socket.gethostname(),
                            " ".join(cmdline))
                    logannotator = LogAnnotator(cmdline)
                    self.exit_code = yield logannotator.start()

                    if self.exit_code == 0:
                        read_and_store_outputs(outputs_file, net=net,
                                operation_id=parsed_arguments.operation_id,
                                parallel_id=parallel_id)

                    else:
                        LOG.info("Non-zero exit-code: %s from perl_wrapper.",
                                self.exit_code)

        except:
            LOG.exception('Error in workflow-wrapper')
            raise


def parse_parallel_id(unparsed_parallel_id):
    if unparsed_parallel_id:
        par_id_as_list = json.loads(unparsed_parallel_id)
    else:
        par_id_as_list = []

    return ParallelIdentifier(par_id_as_list)


def write_inputs(file_object, net, parallel_id, input_connections):
    parsed_input_connections = json.loads(input_connections)
    inputs = io.load_inputs(net=net, input_connections=parsed_input_connections,
            parallel_id=parallel_id)

    LOG.debug('Inputs: %s', inputs)

    json.dump(inputs, file_object)
    file_object.flush()


def read_and_store_outputs(file_obj, net, operation_id, parallel_id):
    outputs = json.load(file_obj)

    io.store_outputs(net=net, operation_id=operation_id,
            outputs=outputs, parallel_id=parallel_id)
