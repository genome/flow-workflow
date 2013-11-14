from flow.util.mkdir import make_path_to

import base
import glob
import os
import re
import shutil
import subprocess
import sys
import unittest

def base_dir():
    return os.path.dirname(os.path.abspath(__file__))

def directory(subdir):
    return os.path.join(base_dir(), subdir)

class CrashingWorkflowTest(base.BaseWorkflowTest):
    @property
    def base_command_line(self):
        return ['flow', 'execute-workflow', '--block',
                '--xml', self.xml_path,
                '--resource-file', self.resources_path]

    @property
    def inputs_path(self):
        return os.path.join(directory('crashing_workflow'), 'inputs.json')

    @property
    def expected_outputs_path(self):
        return os.path.join(directory('crashing_workflow'), 'expected_outputs.json')

    @property
    def perl_directory(self):
        return directory('perl')

    @property
    def config_directory(self):
        return directory('config')

    @property
    def log_dir(self):
        return os.path.join('test_logs', 'crashing_workflow')

    @property
    def xml_path(self):
        return os.path.join(directory('crashing_workflow'), 'workflow.xml')

    @property
    def resources_path(self):
        return os.path.join(directory('crashing_workflow'), 'resources.json')

    def test_workflow(self):
        self.execute_workflow()
        self.verify_errors_are_in_logs()
        self.verify_redis_keys_expired()

    def execute_workflow(self):
        shutil.rmtree(self.log_dir, ignore_errors=True)
        make_path_to(self.stderr_log_file)
        make_path_to(self.stdout_log_file)

        with open(self.stderr_log_file, 'a') as stderr:
            with open(self.stdout_log_file, 'a') as stdout:
                rv = subprocess.call(self.command_line,
                        stderr=stderr, stdout=stdout)
        self.assertEqual(1, rv, 'Workflow crashed as expected')

    def verify_errors_are_in_logs(self):
        for crasher_type in ['Execute', 'Shortcut']:
            with open(self.get_log_file(crasher_type), "r") as log_file:
                self.check_for_error_message_in(log_file, crasher_type)

    def check_for_error_message_in(self, log_file, crasher_type):
        search_string = 'CRASHED AS EXPECTED IN %s' % crasher_type.upper()

        found = False
        for line in log_file:
            if re.search(search_string, line):
                found = True
                break

        self.assertTrue(found,
            'Could not find expected error message for %s' % crasher_type)

    def get_log_file(self, crasher_type):
        glob_string = os.path.join(self.log_dir, '%sCrasher.*.err' % crasher_type)
        files = glob.glob(glob_string)
        if len(files) == 1:
            return files[0]
        else:
            raise RuntimeError("Cannot locate the log for %sCrasher" % crasher_type)



if __name__ == '__main__':
    unittest.main()

