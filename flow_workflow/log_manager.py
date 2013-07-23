import os
import re


LOG_NAME_TEMPLATE = '%(base_name)s.%(operation_id)s.%(suffix)s'
MAX_BASE_NAME_LEN = 256


class LogManager(object):
    def __init__(self, log_dir, operation_id, operation_name):
        self.log_dir = log_dir
        self.operation_id = operation_id
        self.operation_name = operation_name

    @property
    def stderr_log_path(self):
        return self._resolve_log_path(suffix='err')

    @property
    def stdout_log_path(self):
        return self._resolve_log_path(suffix='out')

    @property
    def base_name(self):
        bname = re.sub("[^A-Za-z0-9_.-]+", "_",
                self.operation_name)[:MAX_BASE_NAME_LEN]
        return re.sub("^_*|_*$", "", bname)

    def _resolve_log_path(self, suffix):
        template_args = {
                'base_name': self.base_name,
                'operation_id':self.operation_id,
                'suffix':suffix,
        }
        filename = LOG_NAME_TEMPLATE % template_args
        return os.path.join(self.log_dir, filename)
