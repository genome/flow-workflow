import base
import os
import re
import unittest


class ClassGenerator(object):
    def __init__(self, workflows_directory, module, **kwargs):
        self.workflows_directory = workflows_directory
        self.module = module

        self.pass_through_args = kwargs

    def generate_classes(self):
        for workflow_name in os.listdir(self.workflows_directory):
            ptg = ParticularTestGenerator(workflow_name=workflow_name,
                    workflow_directory=os.path.join(
                        self.workflows_directory, workflow_name),
                    **self.pass_through_args)

            ptg.attach_to(self.module)


class ParticularTestGenerator(object):
    def __init__(self, workflow_name, workflow_directory, **kwargs):
        self.workflow_name = workflow_name
        self.workflow_directory = workflow_directory

        self.pass_through_args = kwargs

    def attach_to(self, module):
        cls = type(self.test_name, (base.BaseWorkflowTest, unittest.TestCase),
                self.class_dict)
        setattr(module, self.test_name, cls)

    @property
    def class_dict(self):
        result = {
            'base_command_line': self.base_command_line,
            'log_dir': self.log_dir,
            'expected_outputs_path': self.expected_outputs_path,
            'inputs_path': self.inputs_path,
        }

        result.update(self.pass_through_args)
        return result

    @property
    def base_command_line(self):
        return ['flow', 'execute-workflow', '--block',
                '--xml', self.xml_path,
                '--resource-file', self.resources_path]


    @property
    def log_dir(self):
        return os.path.join('test_logs', self.workflow_name)

    @property
    def sanitized_workflow_name(self):
        return re.sub('-', '_', self.workflow_name)

    @property
    def test_name(self):
        return self.sanitized_workflow_name + 'Test'

    @property
    def xml_path(self):
        return os.path.join(self.workflow_directory, 'workflow.xml')

    @property
    def inputs_path(self):
        return os.path.join(self.workflow_directory, 'inputs.json')

    @property
    def resources_path(self):
        return os.path.join(self.workflow_directory, 'resources.json')

    @property
    def expected_outputs_path(self):
        return os.path.join(self.workflow_directory, 'expected_outputs.json')
