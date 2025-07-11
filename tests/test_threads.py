import threading
import time
from collections.abc import Generator
from unittest.mock import Mock, patch

import pytest

from pystatsd import StatsDClient


class TestThreadingAndRaceConditions:
    @pytest.fixture
    def mock_socket(self) -> Generator[Mock]:
        with patch('socket.socket') as mock_socket:
            mock_socket.return_value.connect.return_value = None
            mock_socket.return_value.send.return_value = None
            yield mock_socket

    @pytest.fixture
    def slow_client(self, mock_socket: Mock) -> Generator[StatsDClient]:
        client = StatsDClient(
            host='localhost',
            port=8125,
            default_periodic_send_interval_sec=100500,  # Very long to prevent auto-flush
            max_udp_size_bytes=1024,
            app_name='test_app',
        )
        client.init()
        yield client
        client.close()

    def test_concurrent_count_additions_many_iterations(self, slow_client: StatsDClient) -> None:
        def add_counts() -> None:
            for _ in range(100_000):
                slow_client.count('race_test', 1)

        threads = [threading.Thread(target=add_counts) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        metric_key, metric = next(iter(slow_client.count_metrics.items()))
        assert metric_key.name == 'race_test'
        assert metric.value == 200_000

    def test_concurrent_add_and_flush(self, slow_client: StatsDClient) -> None:
        sent_count = 0

        def mock_send(data: bytes) -> None:
            nonlocal sent_count
            for line in data.decode('utf-8').split('\n'):
                if line and 'flush_test' in line:
                    sent_count += 1

        assert slow_client.socket is not None
        slow_client.socket.send = mock_send  # type: ignore [assignment]

        start_event = threading.Event()
        stop_event = threading.Event()

        def add_metrics() -> None:
            start_event.wait()
            for i in range(100_000):
                slow_client.time('flush_test', i)
            stop_event.set()

        def flush_metrics() -> None:
            start_event.wait()
            while not stop_event.wait(0.001):
                slow_client.flush()

        add_thread = threading.Thread(target=add_metrics)
        flush_thread = threading.Thread(target=flush_metrics)

        add_thread.start()
        flush_thread.start()
        start_event.set()
        add_thread.join()
        flush_thread.join()

        slow_client.flush()
        time.sleep(0.5)  # Wait for worker thread

        assert sent_count == 100_000, f'Race condition found! Expected 100000, got {sent_count}'

    def test_concurrent_add_and_worker_flush(self, mock_socket: Mock) -> None:
        sent_count = 0

        fast_client = StatsDClient(
            host='localhost',
            port=8125,
            default_periodic_send_interval_sec=0.001,
            max_udp_size_bytes=1024,
            app_name='test_app',
        )
        fast_client.init()

        def mock_send(data: bytes) -> None:
            nonlocal sent_count
            for line in data.decode('utf-8').split('\n'):
                value = int(line.split('|')[0].split(':')[1])
                sent_count += value

        assert fast_client.socket is not None
        fast_client.socket.send = mock_send  # type: ignore [assignment]

        def add_metrics() -> None:
            for _ in range(100_000):
                fast_client.count('worker_test', 1)

        add_thread = threading.Thread(target=add_metrics)
        add_thread.start()
        add_thread.join()

        time.sleep(0.5)  # Wait for worker thread to process

        assert sent_count == 100_000, f'Race condition found! Expected 100000, got {sent_count}'
        fast_client.close()
