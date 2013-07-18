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
Block = flow_workflow.entities.block.adapter:BlockAdapter
Command = flow_workflow.entities.command.adapter:CommandAdapter
Converge = flow_workflow.entities.converge.adapter:ConvergeAdapter
Event = flow_workflow.entities.event.adapter:EventAdapter
Model = flow_workflow.entities.model.adapter:ModelAdapter
input_storage = flow_workflow.entities.workflow.adapter:InputStorageAdapter
null = flow_workflow.adapter_base:NullAdapter
pass_through = flow_workflow.pass_through.adapters:PassThroughAdapter

[flow_workflow.operations]
block = flow_workflow.operation_base:PassThroughOperation
command = flow_workflow.operation_base:DirectStorageOperation
converge = flow_workflow.operation_base:DirectStorageOperation
event = flow_workflow.operation_base:DirectStorageOperation
input_connector = flow_workflow.entities.model.operations:InputConnectorOperation
model = flow_workflow.entities.model.operations:ModelOperation
null = flow_workflow.operation_base:NullOperation
output_connector = flow_workflow.operation_base:PassThroughOperation
input_storage = flow_workflow.operation_base:DirectStorageOperation
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
