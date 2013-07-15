from unittest import TestCase, main
from flow_workflow.entities.log_manager import LogManager

class LogManagerTests(TestCase):
    def setUp(self):
        self.log_dir = '/exciting/log/dir'
        self.operation_id = 12345
        self.operation_name = 'test_op_name'

        self.log_manager = LogManager(log_dir=self.log_dir,
                operation_id=self.operation_id,
                operation_name=self.operation_name)

    def test_stderr_log_path(self):
        self.assertEqual('/exciting/log/dir/test_op_name.12345.err',
                self.log_manager.stderr_log_path)

    def test_stdout_log_path(self):
        self.assertEqual('/exciting/log/dir/test_op_name.12345.out',
                self.log_manager.stdout_log_path)

if __name__ == '__main__':
    main()
