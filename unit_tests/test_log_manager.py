from unittest import TestCase, main
from flow_workflow.log_manager import LogManager
from flow_workflow.parallel_id import ParallelIdentifier


class LogManagerTests(TestCase):
    def setUp(self):
        self.log_dir = '/exciting/log/dir'
        self.operation_id = 12345
        self.operation_name = 'test_op_name'

        self.log_manager = LogManager(log_dir=self.log_dir,
                operation_id=self.operation_id,
                operation_name=self.operation_name)

    def test_serialize_parallel_id_empty(self):
        parallel_id = ParallelIdentifier()
        self.assertEqual([],
                self.log_manager._serialize_parallel_id(parallel_id))

    def test_serialize_parallel_id_normal(self):
        parallel_id = ParallelIdentifier([(4, 3), (5, 7)])
        self.assertEqual(['4_3', '5_7'],
                self.log_manager._serialize_parallel_id(parallel_id))

    def test_stderr_log_path(self):
        parallel_id = ParallelIdentifier()
        self.assertEqual('/exciting/log/dir/test_op_name.12345.err',
                self.log_manager.stderr_log_path(parallel_id))

    def test_stdout_log_path(self):
        parallel_id = ParallelIdentifier()
        self.assertEqual('/exciting/log/dir/test_op_name.12345.out',
                self.log_manager.stdout_log_path(parallel_id))

    def test_parallel_id_log_path(self):
        parallel_id = ParallelIdentifier([(4, 3), (5, 7)])
        self.assertEqual(
                '/exciting/log/dir/test_op_name.12345.4_3.5_7.out',
                self.log_manager.stdout_log_path(parallel_id))


if __name__ == '__main__':
    main()
