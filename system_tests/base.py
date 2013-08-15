from test_helpers import redistest
from flow.util.mkdir import make_path_to

import abc
import json
import os
import subprocess
import tempfile
import yaml


class BaseWorkflowTest(redistest.RedisTest):
    __metaclass__ = abc.ABCMeta

    @abc.abstractproperty
    def base_command_line(self):
        pass

    @abc.abstractproperty
    def inputs_path(self):
        pass

    @abc.abstractproperty
    def expected_outputs_path(self):
        pass

    @abc.abstractproperty
    def perl_directory(self):
        pass

    @abc.abstractproperty
    def config_directory(self):
        pass

    @abc.abstractproperty
    def log_dir(self):
        pass


    @property
    def stderr_log_file(self):
        return os.path.join(self.log_dir, 'log.err')

    @property
    def stdout_log_file(self):
        return os.path.join(self.log_dir, 'log.out')

    @property
    def encoded_inputs_path(self):
        return os.path.join(self.tmp_dir, 'encoded_inputs_file.json')

    @property
    def outputs_path(self):
        return os.path.join(self.tmp_dir, 'outputs_file.json')

    @property
    def converted_outputs_path(self):
        return os.path.join(self.log_dir, 'outputs_file.json')

    @property
    def command_line(self):
        return self.base_command_line + ['--outputs', self.outputs_path,
                '--inputs', self.encoded_inputs_path]

    @property
    def expected_outputs(self):
        return yaml.load(open(self.expected_outputs_path))

    @property
    def flow_config_path(self):
        return ':'.join([self.tmp_dir, self.config_directory])

    @property
    def temporary_config_path(self):
        return os.path.join(self.tmp_dir, 'flow.yaml')

    @property
    def temporary_configuration(self):
        return {
            'redis': {
                'unix_socket_path': self.redis_unix_socket_path,
            }
        }

    @property
    def encode_inputs_command_line(self):
        return ['perl', os.path.join(self.perl_directory, 'encode_inputs.pl'),
                self.inputs_path, self.encoded_inputs_path]

    def setup_temporary_flow_config_file(self):
        yaml.dump(self.temporary_configuration,
                open(self.temporary_config_path, 'w'))


    def setup_encoded_inputs_file(self):
        subprocess.check_call(self.encode_inputs_command_line)


    def setup_flow_config_path(self):
        self.old_flow_config_path = os.environ.get('FLOW_CONFIG_PATH', '')
        os.environ['FLOW_CONFIG_PATH'] = self.flow_config_path

    def teardown_flow_config_path(self):
        os.environ['FLOW_CONFIG_PATH'] = self.old_flow_config_path


    def setup_perl5lib(self):
        self.old_perl5lib = os.environ['PERL5LIB']
        os.environ['PERL5LIB'] = (self.perl_directory
                + ':' + os.environ['PERL5LIB'])

    def teardown_perl5lib(self):
        os.environ['PERL5LIB'] = self.old_perl5lib


    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()

        self.setup_perl5lib()
        self.setup_flow_config_path()
        self.setup_temporary_flow_config_file()
        self.setup_encoded_inputs_file()

    def tearDown(self):
        self.teardown_flow_config_path()
        self.teardown_perl5lib()
        redistest.RedisTest.tearDown(self)


    def execute_workflow(self):
        make_path_to(self.stderr_log_file)
        make_path_to(self.stdout_log_file)
        with open(self.stderr_log_file, 'a') as stderr:
            with open(self.stdout_log_file, 'a') as stdout:
                rv = subprocess.call(self.command_line,
                        stderr=stderr, stdout=stdout)
        self.assertEqual(0, rv)

    @property
    def read_outputs_command_line(self):
        return ['perl', os.path.join(self.perl_directory, 'decode_outputs.pl'),
                self.outputs_path]

    @property
    def actual_outputs(self):
        p = subprocess.Popen(self.read_outputs_command_line,
                stdout=subprocess.PIPE)
        out, err = p.communicate()
        return yaml.load(out)


    def verify_outputs(self):
        actual_outputs = self.actual_outputs
        json.dump(actual_outputs, open(self.converted_outputs_path, 'w'))
        self.assertEqual(self.expected_outputs, actual_outputs)

    def verify_redis_keys_expired(self):
        failed_to_expire = []
        for key in self.conn.keys():
            if self.conn.ttl(key) is None:
                failed_to_expire.append(key)

        self.assertEqual([], failed_to_expire)

    def test_workflow(self):
        self.execute_workflow()
        self.verify_outputs()
        self.verify_redis_keys_expired()
