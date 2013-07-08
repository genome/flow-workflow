from flow_workflow.operations import base
from flow_workflow.operations.perl_action import actions
from flow_workflow.operations.perl_action import future_nets

import abc
import os
import re


LOG_NAME_TEMPLATE = '%(base_name)s.%(operation_id)s.%(suffix)s'
MAX_BASE_NAME_LEN = 256


class PerlActionAdapterBase(base.AdapterBase):
    # XXX action_type and action_id should be refactored into a data clump
    @abc.abstractproperty
    def action_type(self):
        pass

    @abc.abstractproperty
    def action_id(self):
        pass

    @property
    def log_dir(self):
        return self._log_dir or self.xml.attrib.get('logDir', '.')

    def stderr_log_path(self, operation_id):
        return self._resolve_log_path(suffix='err',
                base_name=self._base_log_file_name(),
                operation_id=operation_id)

    def stdout_log_path(self, operation_id):
        return self._resolve_log_path(suffix='out',
                base_name=self._base_log_file_name(),
                operation_id=operation_id)


    def _base_log_file_name(self):
        base = re.sub("[^A-Za-z0-9_.-]+", "_", self.name)[:MAX_BASE_NAME_LEN]
        return re.sub("^_*|_*$", "", base)

    def _resolve_log_path(self, **kwargs):
        filename = LOG_NAME_TEMPLATE % kwargs
        return os.path.join(self.log_dir, filename)


    def net(self, input_connections, output_properties, resources):
        if self.parallel_by:
            return self._parallel_by_net(input_connections=input_connections,
                    output_properties=output_properties, resources=resources)

        else:
            return self._normal_net(input_connections=input_connections,
                    resources=resources)

    @property
    def parallel_by(self):
        return self.xml.attrib.get('parallelBy')

    def execute_action_class(self, remote_execute):
        if remote_execute:
            return actions.LSFAction
        else:
            return actions.ForkAction

    @property
    def shortcut_action_class(self):
        return actions.ForkAction

    def _parallel_by_net(self, input_connections, output_properties, resources):
        target_net = self._normal_net(input_connections, resources)
        return future_nets.ParallelByNet(target_net, self.parallel_by,
                output_properties=output_properties)

    def _normal_net(self, input_connections, resources):
        return future_nets.PerlActionNet(
                name=self.name,
                operation_id=self.operation_id,
                parent_operation_id=self.parent.operation_id,
                input_connections=input_connections,
                resources=resources,
                stderr=self.stderr_log_path,
                stdout=self.stdout_log_path,
                action_type=self.action_type,
                action_id=self.action_id,
                shortcut_action_class=self.shortcut_action_class,
                execute_action_class=self.execute_action_class(remote_execute=True),
                )
