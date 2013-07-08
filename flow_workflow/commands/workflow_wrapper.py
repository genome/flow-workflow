from flow.commands.base import CommandBase
from flow.configuration.inject.redis_conf import RedisConfiguration
from flow.configuration.settings.injector import setting
from flow.util.logannotator import LogAnnotator
from injector import inject
from twisted.internet import defer

import flow.interfaces
import copy
import json
import logging


LOG = logging.getLogger(__name__)


def _build_command_cmdline(parsed_arguments, inputs_file, outputs_file):
    return ["command", parsed_arguments.method, parsed_arguments.perl_command,
            inputs_file, outputs_file]


def _build_event_cmdline(parsed_arguments, inputs_file, outputs_file):
    return ["event", parsed_arguments.method, parsed_arguments.event_id,
            outputs_file]

CMDLINE_BUILDERS = {
        'command':_build_command_cmdline,
        'event':_build_event_cmdline,
        }

@inject(perl_wrapper=setting('workflow.perl_wrapper'),
        storage=flow.interfaces.IStorage,)
class WorkflowWrapperCommand(CommandBase):
    injector_modules = [RedisConfiguration]

    @staticmethod
    def annotate_parser(parser):
        cmd_parser.add_argument("method", help="shortcut or execute")
        cmd_parser.add_argument('action-type', help='event or command')
        cmd_parser.add_argument('action-id', help='event_id or perl_class')

        cmd_parser.add_argument('net_key', help='used to look up inputs')
        cmd_parser.add_argument('operation-id', type=int,
                help='used to look up inputs')
        cmd_parser.add_argument('input-connections',
                help='used to look up inputs')
        cmd_parser.add_argument('parallel_idx', default=None,
                help='used to look up inputs')


    @defer.inlineCallbacks
    def _execute(self, parsed_arguments):
        net_key = parsed_arguments.net_key
        operation_id = parsed_arguments.operation_id
        input_connections = json.loads(parsed_arguments.input_connections)
        parallel_idx = parsed_arguments.parallel_idx

        net = rom.get_object(parsed_arguments.net_key)
        # load inputs from redis
        inputs = self._load_inputs(storage=storage,
                net=net, operation_id=operation_id,
                input_connections=input_connections,
                parallel_idx=parallel_idx)

        with NamedTemporaryFile() as inputs_file:
            with NamedTemporaryFile() as outputs_file:
                # write inputs to file
                self._write_inputs(inputs, inputs_file)

                cmdline = copy.copy(self.perl_wrapper)
                cmdline_builder = CMDLINE_BUILDERS[parsed_arguments.action_type]
                cmdline.extend(cmdline_builder(parsed_arguments, inputs_file,
                    outputs_file))

                # XXX future
                #process_monitor = ProcessMonitor(os.getpid())
                #process_monitor.start()

                LOG.info("On host %s: executing %s", platform.node(),
                        " ".join(cmdline))
                logannotator = LogAnnotator(cmdline)
                self.exit_code = yield logannotator.start()

                if self.exit_code == 0:
                    # read outputs from file
                    outputs = self._read_outputs(outputs_file)

                    # store outputs in redis
                    self._store_outputs(storage=storage, net=net,
                            operation_id=operation_id, outputs=outputs,
                            parallel_idx=parallel_idx)
                else:
                    LOG.warning("Non-zero exit-code: %s from perl_wrapper.",
                            self.exit_code)

        @staticmethod
        def _load_inputs(storage, net, operation_id, input_connections, parallel_idx):
            inputs = io.load_input(net=net,
                input_connections=input_connections,
                parallel_index=parallel_index)
            LOG.debug("Input values: %s", inputs)
            return inputs

        @staticmethod
        def _write_inputs(inputs, infile):
            json.dump(inputs, infile)
            infile.flush()

        @staticmethod
        def _read_outputs(outfile):
            return json.load(outfile)

        @staticmethod
        def _store_outputs(storage, net, operation_id, outputs, parallel_idx):
            outputs = io.store_outputs(net=net, operation_id=operation_id,
                    outputs=outputs, parallel_idx=parallel_idx)
