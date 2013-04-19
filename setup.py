from setuptools import setup, find_packages

entry_points = '''
[flow.commands]
submit-workflow = flow_workflow.commands.submit_workflow:SubmitWorkflowCommand
workflow-historian-service = flow_workflow.historian.command:WorkflowHistorianCommand
workflow-wrapper = flow_workflow.commands.workflow_wrapper:WorkflowWrapperCommand

[flow.services]
workflow_historian = flow_workflow.historian.service_interface:WorkflowHistorianServiceInterface
'''

setup(
        name = 'flow_workflow',
        version = '0.1',
        packages = find_packages(exclude=['unit_tests']),
        entry_points = entry_points,
        setup_requires = [
            'nose',
        ],
        install_requires = [
            'cx_Oracle',
            'sqlalchemy',
            'hiredis',
            'redis',
            'lxml',
        ],
        tests_require = [
            'coverage',
            'fakeredis',
            'mock',
            'nose',
        ],
        test_suite = 'unit_tests',
)
