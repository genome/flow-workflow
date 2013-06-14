import fakeredis
import mock


class TestGenomeActionMixin(object):
    def setUp(self):
        self.connection = fakeredis.FakeRedis()

        self.operation_id = 12345
        self.parallel_idx = 42

        self.net = mock.MagicMock()
        self.net.key = 'netkey!'

        self.color_descriptor = mock.Mock()
        self.service_interfaces = {
            'orchestrator': mock.Mock(),
        }



    def tearDown(self):
        self.connection.flushall()
