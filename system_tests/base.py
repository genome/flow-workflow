import abc
import json
import os
import redis
import subprocess
import tempfile
import yaml


class BaseWorkflowTest(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractproperty
    def base_command_line(self):
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

    @property
    def outputs_path(self):
        return os.path.join(self.tmp_dir, 'outputs_file.json')

    @property
    def command_line(self):
        return self.base_command_line + ['--outputs', self.outputs_path]

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
                'unix_socket_path': os.environ['FLOW_TEST_REDIS_SOCKET'],
            }
        }

    def setup_temporary_flow_config_file(self):
        yaml.dump(self.temporary_configuration,
                open(self.temporary_config_path, 'w'))


    def flush_redis(self):
        conn = redis.Redis(
                unix_socket_path=os.environ['FLOW_TEST_REDIS_SOCKET'])
        conn.flushall()


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


    def setup_redis(self):
        self.flush_redis()

    def teardown_redis(self):
        self.flush_redis()


    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()

        self.setup_perl5lib()
        self.setup_redis()
        self.setup_flow_config_path()
        self.setup_temporary_flow_config_file()

    def tearDown(self):
        self.teardown_flow_config_path()
        self.teardown_redis()
        self.teardown_perl5lib()


    def execute_workflow(self):
        rv = subprocess.call(self.command_line)
        self.assertEqual(0, rv)

    @property
    def read_outputs_command_line(self):
        return ['perl', os.path.join(self.perl_directory, 'slurp_outputs.pl'),
                self.outputs_path]

    @property
    def actual_outputs(self):
        p = subprocess.Popen(self.read_outputs_command_line,
                stdout=subprocess.PIPE)
        out, err = p.communicate()
        return json.loads(out)


    def verify_outputs(self):
        self.assertEqual(self.expected_outputs, self.actual_outputs)


    def test_workflow(self):
        self.execute_workflow()
        self.verify_outputs()
