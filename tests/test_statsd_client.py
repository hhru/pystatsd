import time
from collections.abc import Generator
from unittest.mock import Mock, patch

import pytest

from pystatsd import Metric, MetricKey, MetricType, StatsDClient


class TestMetric:
    def test_metric_to_statsd_format(self) -> None:
        key = MetricKey(MetricType.COUNT, 'requests', 'myapp', {'status': '200'})
        metric = Metric(key, 100)
        assert metric.to_statsd_format() == 'requests.status_is_200.app_is_myapp:100|c'

    def test_metric_to_statsd_format_no_tags(self) -> None:
        key = MetricKey(MetricType.TIME, 'latency', 'myapp', {})
        metric = Metric(key, 3.71)
        assert metric.to_statsd_format() == 'latency.app_is_myapp:3.71|ms'

    def test_metric_get_size_bytes(self) -> None:
        key = MetricKey(MetricType.GAUGE, 'memory', 'myapp', {'host': 'server1'})
        metric = Metric(key, 1024)
        expected_size = len(metric.to_statsd_format().encode('utf-8'))

        assert metric.get_size_bytes() == expected_size

    def test_metric_hash_and_equality(self) -> None:
        key1 = MetricKey(MetricType.COUNT, 'requests', 'myapp', {'tag1': 'aa'})
        key2 = MetricKey(MetricType.COUNT, 'requests', 'myapp', {'tag1': 'aa'})
        key3 = MetricKey(MetricType.COUNT, 'requests', 'myapp', {'tag1': 'bb'})

        assert key1 == key2
        assert hash(key1) == hash(key2)

        assert key1 != key3
        assert hash(key1) != hash(key3)


class TestStatsDClient:
    @pytest.fixture
    def mock_socket(self) -> Generator[Mock]:
        with patch('socket.socket') as mock_socket:
            mock_socket.return_value.connect.return_value = None
            mock_socket.return_value.send.return_value = None
            yield mock_socket

    @pytest.fixture
    def statsd_client(self, mock_socket: Mock) -> Generator[StatsDClient]:
        client = StatsDClient(
            host='localhost',
            port=8125,
            default_periodic_send_interval_sec=100500,
            max_udp_size_bytes=1024,
            app_name='test_app',
        )
        yield client
        client.close()

    def test_count_metric_aggregation(self, statsd_client: StatsDClient) -> None:
        statsd_client.count('requests', 1, tag1='aa')
        statsd_client.count('requests', 2, tag1='aa')
        statsd_client.count('requests', 3, tag1='aa')

        assert len(statsd_client.count_metrics) == 1

        metric_key, metric = next(iter(statsd_client.count_metrics.items()))
        assert metric_key.name == 'requests'
        assert metric_key.type == MetricType.COUNT
        assert metric_key.tags == {'tag1': 'aa'}
        assert metric_key.app_name == 'test_app'
        assert metric.value == 6

    def test_different_tags_not_aggregated(self, statsd_client: StatsDClient) -> None:
        statsd_client.count('requests', 1, tag1='aa')
        statsd_client.count('requests', 2, tag1='bb')

        assert len(statsd_client.count_metrics) == 2

    def test_time_metrics_not_aggregated(self, statsd_client: StatsDClient) -> None:
        statsd_client.time('latency', 1.0, tag1='/api')
        statsd_client.time('latency', 2.0, tag1='/api')

        assert len(statsd_client.time_gauge_metrics) == 2

        metrics = [m for m in statsd_client.time_gauge_metrics if m.key.name == 'latency']
        assert len(metrics) == 2
        assert metrics[0].value == 1.0
        assert metrics[1].value == 2.0

    def test_gauge_metrics_not_aggregated(self, statsd_client: StatsDClient) -> None:
        statsd_client.gauge('memory', 100, tag1='server1')
        statsd_client.gauge('memory', 200, tag1='server1')

        assert len(statsd_client.time_gauge_metrics) == 2

        metrics = [m for m in statsd_client.time_gauge_metrics if m.key.name == 'memory']
        assert len(metrics) == 2
        assert metrics[0].value == 100
        assert metrics[1].value == 200

    def test_empty_tags(self, statsd_client: StatsDClient) -> None:
        statsd_client.count('simple_metric', 1)

        assert len(statsd_client.count_metrics) == 1
        metric_key, metric = next(iter(statsd_client.count_metrics.items()))

        assert metric_key.tags == {}
        assert metric.to_statsd_format() == 'simple_metric.app_is_test_app:1|c'

    def test_flush_clears_buffer(self, statsd_client: StatsDClient) -> None:
        statsd_client.count('requests', 1)
        statsd_client.time('latency', 1.0)
        assert len(statsd_client.count_metrics) == 1
        assert len(statsd_client.time_gauge_metrics) == 1

        statsd_client.flush()
        assert len(statsd_client.count_metrics) == 0
        assert len(statsd_client.time_gauge_metrics) == 0

    def test_size_based_flushing(self, statsd_client: StatsDClient) -> None:
        small_buffer_client = StatsDClient(
            host='localhost',
            port=8125,
            default_periodic_send_interval_sec=100500,
            max_udp_size_bytes=50,
            app_name='test_app',
        )

        small_buffer_client.count('metric1', 1)
        small_buffer_client.count('metric2', 1)

        assert len(small_buffer_client.count_metrics) == 1  # because buffer was sent, and new metric put to new buffer
        small_buffer_client.close()

    def test_large_metric_dropped(self, statsd_client: StatsDClient) -> None:
        small_buffer_client = StatsDClient(
            host='localhost',
            port=8125,
            default_periodic_send_interval_sec=100500,
            max_udp_size_bytes=10,
            app_name='test_app',
        )

        small_buffer_client.count('very_long_metric_name_that_exceeds_limit', 1)
        assert len(small_buffer_client.count_metrics) == 0
        small_buffer_client.close()

    def test_close_method(self, statsd_client: StatsDClient) -> None:
        statsd_client.count('test', 1)
        statsd_client.time('latency', 1.0)
        assert len(statsd_client.count_metrics) == 1
        assert len(statsd_client.time_gauge_metrics) == 1

        statsd_client.close()
        assert len(statsd_client.count_metrics) == 0
        assert len(statsd_client.time_gauge_metrics) == 0

    def test_periodic_flushing(self, mock_socket: Mock) -> None:
        client = StatsDClient(
            host='localhost',
            port=8125,
            default_periodic_send_interval_sec=0.5,
            max_udp_size_bytes=1024,
            app_name='test_app',
        )

        client.count('test', 1)
        client.time('latency', 1.0)
        assert len(client.count_metrics) == 1
        assert len(client.time_gauge_metrics) == 1

        time.sleep(1.0)

        assert len(client.count_metrics) == 0
        assert len(client.time_gauge_metrics) == 0
        client.close()

    def test_socket_connection_error(self) -> None:
        with patch('socket.socket') as mock_socket:
            mock_socket.return_value.connect.side_effect = OSError('Connection refused')

            client = StatsDClient(
                host='invalid_host',
                port=8125,
                default_periodic_send_interval_sec=100500,
                max_udp_size_bytes=1024,
                app_name='test_app',
            )

            client.count('test1', 1)
            client.count('test2', 2)
            assert len(client.count_metrics) == 2

            client.flush()

            # Buffer should be cleared after failed flush attempt
            assert len(client.count_metrics) == 0

            # Add new metrics - they should start a fresh buffer
            client.count('new_test1', 4)
            client.count('new_test2', 5)
            assert len(client.count_metrics) == 2

            client.close()
