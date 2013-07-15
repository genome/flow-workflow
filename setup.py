from setuptools import setup, find_packages

entry_points = '''
[flow.commands]
submit-workflow = flow_workflow.commands.submit_workflow:SubmitWorkflowCommand
execute-workflow = flow_workflow.commands.execute_workflow:ExecuteWorkflowCommand
workflow-historian-service = flow_workflow.historian.command:WorkflowHistorianCommand
workflow-wrapper = flow_workflow.commands.workflow_wrapper:WorkflowWrapperCommand

[flow.services]
workflow_historian = flow_workflow.historian.service_interface:WorkflowHistorianServiceInterface
workflow_completion = flow_workflow.completion:WorkflowCompletionServiceInterface

[flow_workflow.adapters]
Block = flow_workflow.operations.block.adapter:BlockAdapter
Command = flow_workflow.operations.command.adapter:CommandAdapter
Converge = flow_workflow.operations.converge.adapter:ConvergeAdapter
Event = flow_workflow.operations.event.adapter:EventAdapter
Model = flow_workflow.operations.model.adapter:ModelAdapter
input_connector = flow_workflow.operations.connectors.adapter:InputConnector
output_connector = flow_workflow.operations.connectors.adapter:OutputConnector
null = flow_workflow.operations.base:NullAdapter
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
