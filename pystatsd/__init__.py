from __future__ import annotations

import logging
import queue
import socket
import threading
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from itertools import chain
from typing import Union

NEWLINE_SIZE = len(b'\n')

MetricValue = Union[int, float]

log = logging.getLogger('statsd_client')


class MetricType(Enum):
    COUNT = 'c'
    TIME = 'ms'
    GAUGE = 'g'


@dataclass(frozen=True)
class MetricKey:
    __slots__ = ('app_name', 'name', 'tags', 'type')

    type: MetricType
    name: str
    app_name: str
    tags: dict[str, str]

    def __hash__(self) -> int:
        return hash((self.type, self.name, tuple(sorted(self.tags.items())), self.app_name))

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, MetricKey):
            return False
        return (
            self.type == other.type
            and self.name == other.name
            and self.tags == other.tags
            and self.app_name == other.app_name
        )


@dataclass
class Metric:
    __slots__ = ('key', 'value')

    key: MetricKey
    value: MetricValue

    def to_statsd_format(self) -> str:
        all_tags = {**self.key.tags, 'app': self.key.app_name}

        tags_str = _convert_tags(all_tags)
        return f'{self.key.name}{tags_str}:{self.value}|{self.key.type.value}'

    def get_size_bytes(self) -> int:
        metric_str = self.to_statsd_format()
        return len(metric_str.encode('utf-8'))


def _convert_tag(name: str, value: str) -> str:
    return '{}_is_{}'.format(name.replace('.', '-'), str(value).replace('.', '-'))


def _convert_tags(tags: dict[str, str]) -> str:
    if not tags:
        return ''

    return '.' + '.'.join(_convert_tag(name, value) for name, value in tags.items() if value is not None)


class StatsDClientABC(ABC):
    @abstractmethod
    def flush(self) -> None:
        pass

    @abstractmethod
    def count(self, name: str, value: int, **kwargs: str) -> None:
        pass

    @abstractmethod
    def time(self, name: str, millis: float, **kwargs: str) -> None:
        pass

    @abstractmethod
    def gauge(self, name: str, value: float, **kwargs: str) -> None:
        pass


class StatsDClientStub(StatsDClientABC):
    def __init__(self) -> None:
        pass

    def flush(self) -> None:
        pass

    def count(self, name: str, value: int, **kwargs: str) -> None:
        pass

    def time(self, name: str, millis: float, **kwargs: str) -> None:
        pass

    def gauge(self, name: str, value: float, **kwargs: str) -> None:
        pass


class StatsDClient(StatsDClientABC):
    def __init__(
        self,
        host: str,
        port: int,
        app_name: str,
        default_periodic_send_interval_sec: float = 60,
        max_udp_size_bytes: int = 1024,
        reconnect_timeout_sec: float = 2,
    ) -> None:
        self.host = host
        self.port = port
        self.default_periodic_send_interval_sec = default_periodic_send_interval_sec
        self.app_name = app_name
        self.max_udp_size_bytes = max_udp_size_bytes
        self.reconnect_timeout_sec = reconnect_timeout_sec

        # Accumulation buffers (user operations add here)
        self.count_metrics: dict[MetricKey, Metric] = {}
        self.time_gauge_metrics: list[Metric] = []
        self._cached_total_size_bytes: int = 0

        # Queue for immediate sending when size limit is reached
        self.send_queue: queue.Queue[str] = queue.Queue()

        # Threading coordination
        self._metrics_lock = threading.Lock()
        self._socket_lock = threading.Lock()
        self._shutdown_event = threading.Event()

        self.socket: socket.socket | None = None
        self._connect()
        threading.Thread(target=self._periodic_flush_worker, daemon=True).start()

    def _connect(self) -> None:
        with self._socket_lock:
            log.info('statsd: connecting to %s:%d', self.host, self.port)

            self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.socket.setblocking(False)  # noqa: FBT003

            try:
                self.socket.connect((self.host, self.port))
            except OSError as e:
                log.warning('statsd: connect error: %s', e)
                self._reconnect()

    def _reconnect(self) -> None:
        if self.socket is not None:
            self.socket.close()
        self.socket = None
        if self._shutdown_event.is_set():
            return
        threading.Timer(self.reconnect_timeout_sec, self._connect).start()

    def _periodic_flush_worker(self) -> None:
        while not self._shutdown_event.is_set():
            try:
                data = self.send_queue.get(timeout=self.default_periodic_send_interval_sec)
                if data:
                    with self._socket_lock:
                        self.__write(data)
            except queue.Empty:  # noqa: PERF203 send current metrics by timeout
                self.flush()

    def __add_metric_size(self, metric: Metric) -> None:
        self._cached_total_size_bytes += metric.get_size_bytes() + NEWLINE_SIZE

    def __remove_metric_size(self, metric: Metric) -> None:
        self._cached_total_size_bytes -= metric.get_size_bytes() + NEWLINE_SIZE

    def __collect_current_metrics(self) -> str:
        # must be called within self._metrics_lock
        return '\n'.join(
            metric.to_statsd_format() for metric in chain(self.count_metrics.values(), self.time_gauge_metrics)
        )

    def __write(self, data: str) -> None:
        # must be called within self._socket_lock
        if self.socket is None:
            log.error('statsd: trying to write to closed socket, dropping')
            return

        try:
            self.socket.send(data.encode('utf-8'))
        except OSError as e:
            log.warning('statsd: writing error: %s', e)
            self._reconnect()

    def flush(self) -> None:
        with self._metrics_lock:
            if not self.count_metrics and not self.time_gauge_metrics:
                return

            self.__put_metrics_to_queue()

    def __put_metrics_to_queue(self) -> None:
        data: str = self.__collect_current_metrics()
        self.send_queue.put(data)
        self.count_metrics.clear()
        self.time_gauge_metrics.clear()
        self._cached_total_size_bytes = 0

    def count(self, name: str, value: int, **kwargs: str) -> None:
        self.__add_metric(MetricType.COUNT, name, value, kwargs)

    def time(self, name: str, millis: float, **kwargs: str) -> None:
        self.__add_metric(MetricType.TIME, name, millis, kwargs)

    def gauge(self, name: str, value: float, **kwargs: str) -> None:
        self.__add_metric(MetricType.GAUGE, name, value, kwargs)

    def __add_metric(self, metric_type: MetricType, name: str, value: MetricValue, tags: dict[str, str]) -> None:
        metric_key = MetricKey(metric_type, name, self.app_name, tags)
        metric = Metric(metric_key, value)

        metric_size = metric.get_size_bytes()
        if metric_size > self.max_udp_size_bytes:
            log.error('statsd: metric %s: %s is too long, dropping', name, value)
            return

        with self._metrics_lock:
            if self._cached_total_size_bytes + metric_size > self.max_udp_size_bytes:
                self.__put_metrics_to_queue()

            if metric_type == MetricType.COUNT:
                if metric_key in self.count_metrics:
                    existing_metric = self.count_metrics[metric_key]
                    self.__remove_metric_size(existing_metric)
                    existing_metric.value += value
                    self.__add_metric_size(existing_metric)
                else:
                    self.count_metrics[metric_key] = metric
                    self.__add_metric_size(metric)
            else:
                self.time_gauge_metrics.append(metric)
                self.__add_metric_size(metric)

    def close(self) -> None:
        self._shutdown_event.set()
        self.flush()
        self.send_queue.put('')
