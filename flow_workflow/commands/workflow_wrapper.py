from flow import redisom as rom
from flow.commands.base import CommandBase
from flow.configuration.inject.redis_conf import RedisConfiguration
from flow.configuration.settings.injector import setting
from flow.util.logannotator import LogAnnotator
from flow.util.exit import exit_process
from flow.exit_codes import EXECUTE_ERROR
from flow_workflow import factory
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

        parser.add_argument('--parallel-id', default='[]',
                help='used to look up inputs')

    def _execute(self, parsed_arguments):
        net = rom.get_object(self.storage, parsed_arguments.net_key)

        parallel_id = ParallelIdentifier.deserialize(
                parsed_arguments.parallel_id)

        _execute_deferred = defer.Deferred()

        inputs_file = NamedTemporaryFile()
        outputs_file = NamedTemporaryFile()
        write_inputs(inputs_file, net=net, parallel_id=parallel_id,
                operation_id=parsed_arguments.operation_id)

        cmdline = copy.copy(self.perl_wrapper)
        cmdline_builder = CMDLINE_BUILDERS[
                parsed_arguments.action_type]
        cmdline.extend(cmdline_builder(parsed_arguments.method,
            parsed_arguments.action_id, inputs_file, outputs_file))


        LOG.info('Executing (%s): %s', socket.gethostname(),
                " ".join(cmdline))
        logannotator = LogAnnotator(cmdline)
        deferred = logannotator.start()
        deferred.addCallbacks(self._finish, self._exit,
                callbackKeywords={'parsed_arguments':parsed_arguments,
                    'inputs_file':inputs_file,
                    'outputs_file':outputs_file,
                    'parallel_id':parallel_id,
                    'net':net,
                    '_execute_deferred':_execute_deferred,
        })
        deferred.addErrback(self._exit)

        return _execute_deferred

    def _finish(self, exit_code, parsed_arguments, inputs_file, outputs_file, parallel_id,
            net, _execute_deferred):
        self.exit_code = exit_code
        if exit_code == 0:
            read_and_store_outputs(outputs_file, net=net,
                    operation_id=parsed_arguments.operation_id,
                    parallel_id=parallel_id)
        else:
            LOG.info("Non-zero exit-code: %s from perl_wrapper.", exit_code)

        inputs_file.close()
        outputs_file.close()
        _execute_deferred.callback(None)

        return exit_code

    def _exit(self, error):
        LOG.critical("Unexpected error in workflow-wrapper:\n%s",
                error.getTraceback())
        exit_process(EXECUTE_ERROR)


def write_inputs(file_object, net, parallel_id, operation_id):
    operation = factory.load_operation(net, operation_id)
    inputs = operation.load_inputs(parallel_id)

    LOG.debug('Inputs: %s', inputs)

    json.dump(inputs, file_object)
    file_object.flush()


def read_and_store_outputs(file_obj, net, operation_id, parallel_id):
    outputs = json.load(file_obj)

    io.store_outputs(net=net, operation_id=operation_id,
            outputs=outputs, parallel_id=parallel_id)
