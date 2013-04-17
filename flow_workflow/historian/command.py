from flow.commands.service import ServiceCommand
from flow.configuration.inject.broker import BrokerConfiguration
from flow_workflow.configuration.inject.oltp import OLTPConfiguration
from flow_workflow.historian.handler import WorkflowHistorianMessageHandler
from injector import inject

import logging

LOG = logging.getLogger(__name__)


class WorkflowHistorianCommand(ServiceCommand):
    injector_modules = [
            BrokerConfiguration,
            OLTPConfiguration,
    ]

    def __call__(self, *args, **kwargs):
        self.handlers = [self.injector.get(WorkflowHistorianMessageHandler)]

        return ServiceCommand.__call__(self, *args, **kwargs)
