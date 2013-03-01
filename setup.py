from setuptools import setup, find_packages

entry_points = '''
[flow.commands]
submit-workflow = flow_workflow.commands.submit_workflow:SubmitWorkflowCommand
workflow_historian_service = flow.commands.service:ServiceCommand

[flow.factories]
workflow_historian_create_operation_handler = flow_workflow.historian.handler:CreateOperationMessageHandler
workflow_historian_update_operation_handler = flow_workflow.historian.handler:UpdateOperationMessageHandler
workflow_historian_service_interface = flow_workflow.historian.client:WorkflowHistorianClient

[flow.protocol.message_classes]
workflow_historian_create_operation_message = flow_workflow.historian.messages:CreateOperationMessage
workflow_historian_update_operation_message = flow_workflow.historian.messages:UpdateOperationMessage

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
