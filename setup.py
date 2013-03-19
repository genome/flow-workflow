from setuptools import setup, find_packages

entry_points = '''
[flow.commands]
submit-workflow = flow_workflow.commands.submit_workflow:SubmitWorkflowCommand
workflow_historian_service = flow.commands.service:ServiceCommand

[flow.protocol.message_classes]
workflow_historian_message = flow_workflow.historian.messages:UpdateMessage
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
