from setuptools import setup, find_packages

entry_points = '''
[flow.commands]
submit-workflow = flow_workflow.commands.submit_workflow:SubmitWorkflowCommand
workflow_historian_service = flow.commands.service:ServiceCommand

[flow.factories]
workflow_historian_message_handler = flow_workflow.historian.handler:WorkflowHistorianMessageHandler
workflow_historian_service_interface = flow_workflow.historian.service_interface:WorkflowHistorianServiceInterface
workflow_historian_storage = flow_workflow.historian.storage:WorkflowHistorianStorage

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
