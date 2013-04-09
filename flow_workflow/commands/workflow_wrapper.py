from flow.commands.base import CommandBase
from flow.util.logannotator import LogAnnotator

import json
import copy
import logging
import subprocess

LOG = logging.getLogger(__name__)


def _build_command_cmdline(parsed_arguments, inputs_file, outputs_file):
    return ["command", parsed_arguments.method, parsed_arguments.perl_command,
            inputs_file, outputs_file]


def _build_event_cmdline(parsed_arguments, inputs_file, outputs_file):
    return ["event", parsed_arguments.method, parsed_arguments.event_id, outputs_file]


class WorkflowWrapperCommand(CommandBase):
    def __init__(self, perl_wrapper=[]):
        self.perl_wrapper = perl_wrapper

    @staticmethod
    def annotate_parser(parser):
        subparsers = parser.add_subparsers()
        cmd_parser = subparsers.add_parser("command")
        cmd_parser.add_argument("method", help="shortcut or execute")
        cmd_parser.add_argument("perl_command", help="The genome command class")
        cmd_parser.add_argument("--inputs-file", default=None,
                help="Path to a file containing inputs in json format")
        cmd_parser.add_argument("--outputs-file", default=None,
                help="Path to a file containing outputs in json format")
        cmd_parser.add_argument("--parallel-by")
        cmd_parser.add_argument("--parallel-by-index", type=int)
        cmd_parser.add_argument("--reply", action="store_true", default=False)
        cmd_parser.set_defaults(build_cmdline=_build_command_cmdline)

        event_parser = subparsers.add_parser("event")
        event_parser.add_argument("method", help="shortcut or execute")
        event_parser.add_argument("event_id", help="The event id")
        event_parser.add_argument("--inputs-file", default=None,
                help="Path to a file containing inputs in json format")
        event_parser.add_argument("--outputs-file", default=None,
                help="Path to a file containing outputs in json format")
        event_parser.set_defaults(build_cmdline=_build_event_cmdline)

    def __call__(self, parsed_arguments):
        cmdline = copy.copy(self.perl_wrapper)

        if parsed_arguments.inputs_file is None:
            LOG.debug('No inputs file specified, using /dev/null')
            parsed_arguments.inputs_file = "/dev/null"

        cmdline.extend(parsed_arguments.build_cmdline(parsed_arguments,
                parsed_arguments.inputs_file, parsed_arguments.outputs_file))

        LOG.info("Calling perl wrapper: %s", cmdline)
        log_annotator = LogAnnotator(cmdline)
        exit_code = log_annotator.start()

        if exit_code == 0:
            outputs = json.load(open(parsed_arguments.outputs_file))

        LOG.debug('Perl wrapper exited with code: %d', exit_code)

        return exit_code
