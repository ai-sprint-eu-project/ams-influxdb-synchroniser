# coding: utf-8

from datetime import datetime
from itertools import filterfalse
import logging
from logging import Logger
import re
from time import sleep
from typing import Any, Generator, NoReturn, Optional

from influxdb_client import InfluxDBClient, Point  # type: ignore
from influxdb_client.client import flux_table  # type: ignore
from influxdb_client.client.flux_table import FluxRecord  # type: ignore
from influxdb_client.client.query_api import QueryApi  # type: ignore
from influxdb_client.rest import ApiException  # type: ignore

from .influx_config import InfluxConfig


_logger: Logger = logging.getLogger(__name__)


class SyncController:

    _DEFAULT_MAIN_LOOP_SLEEP_TIME: float = 1
    _DEFAULT_TEST_METRIC_NAME: str = '__test'
    _DEFAULT_TEST_FIELD_NAME: str = 'test'
    _DEFAULT_TEST_FIELD_VALUE: Any = 0
    _DEFAULT_REMOTE_INFLUX_WAIT_SLEEP_TIME: float = 1
    _DEFAULT_WRITE_PRECISION: str = 'ns'
    _REPLICATION_TAG: str = '__replication_marker'
    _REPLICATION_VALUE: str = '_'

    def __init__(self, local_config: InfluxConfig, global_config: InfluxConfig,
                 max_time_range: str = '-24h',
                 write_precision: Optional[str] = None,
                 additional_tags: Optional[dict[str, str]] = None):

        self._logger: Logger = _logger.getChild(__class__.__name__)

        if additional_tags:
            pattern = re.compile('^[A-Za-z][0-9A-Za-z_-]*$')
            for tag_name in additional_tags:
                if (
                    tag_name in [self._REPLICATION_TAG]
                    or pattern.match(tag_name) is None
                ):
                    msg = f'forbidden tag name "{tag_name}"'
                    self._logger.error(msg)
                    raise RuntimeError(msg)

        self._local_config: InfluxConfig = local_config
        self._remote_config: InfluxConfig = global_config
        self._max_time_range: str = max_time_range
        self._main_loop_sleep_time: float = self._DEFAULT_MAIN_LOOP_SLEEP_TIME
        self._remote_influxdb_wait_sleep_time: float = (
                self._DEFAULT_REMOTE_INFLUX_WAIT_SLEEP_TIME)
        self._write_precision: str = (
            write_precision or self._DEFAULT_WRITE_PRECISION
        )
        self._additional_tags: dict[str, str] = additional_tags or {}
        self._test_metric_name: str = self._DEFAULT_TEST_METRIC_NAME
        self._test_field_name: str = self._DEFAULT_TEST_FIELD_NAME
        self._test_field_value: Any = self._DEFAULT_TEST_FIELD_VALUE

        self._local_client: InfluxDBClient = InfluxDBClient(
            url=self._local_config.address,
            token=self._local_config.token,
            org=self._local_config.organisation
        )
        self._remote_client: InfluxDBClient = InfluxDBClient(
            url=self._remote_config.address,
            token=self._remote_config.token,
            org=self._remote_config.organisation
        )
        self._last_synchronised_ts: Optional[datetime] = None

        self._logger.info(f'local InfluxDB config: {self._local_config}')
        self._logger.info(f'remote InfluxDB config: {self._remote_config}')
        self._logger.info(f'max range: {self._max_time_range}')
        self._logger.info(f'tags: {self._additional_tags}')

    def init(self) -> None:
        self._raise_if_local_unavailable(init=True)
        self.test_token_to_local()

        if not self.is_remote_working():
            self._logger.info('waiting for remote InfluxDB instance to become'
                              ' available...')
            while not self.is_remote_working():
                try:
                    sleep(self._remote_influxdb_wait_sleep_time)
                except KeyboardInterrupt:
                    self._logger.info('keyboard interrupt')
                    raise
            self._logger.info('... remote InfluxDB instance is available')
            self._raise_if_local_unavailable(init=False)

        self.test_token_to_remote()

        self._last_synchronised_ts = self._get_most_recent_remote_time()

        self._logger.info('init ok')

    def run(self) -> NoReturn:
        while True:
            try:
                sleep(self._main_loop_sleep_time)
            except KeyboardInterrupt:
                self._logger.info('keyboard interrupt')
                raise
            self._raise_if_local_unavailable(init=False)
            if not self.is_remote_working():
                self._logger.info('remote InfluxDB is not available')
                continue
            if not self.sync_required():
                self._logger.debug('sync is not required')
                continue
            self._send_one_batch_and_update_cache()

    def is_local_working(self) -> bool:
        return self._is_influx_working(client=self._local_client,
                                       label='local_client')

    def is_remote_working(self) -> bool:
        return self._is_influx_working(client=self._remote_client,
                                       label='remote_client')

    def test_token_to_local(self):
        self._raise_if_no_read_access(client=self._local_client,
                                      config=self._local_config, label='local')
        self._logger.debug('token to local InfluxDB is ok')

    def test_token_to_remote(self):
        kwargs = {'client': self._remote_client,
                  'config': self._remote_config, 'label': 'remote'}
        self._raise_if_no_read_access(**kwargs)
        self._raise_if_no_write_access(**kwargs)
        self._logger.debug('token to remote InfluxDB is ok')

    def sync_required(self) -> bool:
        ts = self._get_most_recent_local_time()
        if ts is None:
            self._logger.debug(f'sync not required ({ts=})')
            return False
        if self._last_synchronised_ts is None:
            self._last_synchronised_ts = self._get_most_recent_remote_time()
        if self._last_synchronised_ts is None:
            self._logger.debug(f'sync required'
                               f' ({ts=} {self._last_synchronised_ts=})')
            return True
        result = (ts > self._last_synchronised_ts)
        self._logger.debug(f'sync required: {result=}'
                           f' ({ts=} {self._last_synchronised_ts=})')
        return result

    def _is_influx_working(self, *, client: InfluxDBClient,
                           label: str) -> bool:
        self._logger.debug(f'{label}.ping()...')
        result: bool = client.ping()
        self._logger.debug(f'... {result=}')
        return result

    def _raise_if_local_unavailable(self, *, init: bool) -> None:
        if not self.is_local_working():
            msg: str = 'local InfluxDB {}'.format('is not available'
                                                  if init else
                                                  'has crashed')
            self._logger.error(msg)
            raise RuntimeError(msg)

    def _raise_if_no_read_access(self, *, client: InfluxDBClient,
                                 config: InfluxConfig, label: str) -> None:
        query = ('from(bucket: bucket)'
                 ' |> range(start: duration(v: time_range))'
                 ' |> keep(columns: ["_time"]) |> group() |> limit(n: 1)')
        params = {'bucket': config.bucket,
                  'time_range': self._max_time_range}
        self._logger.debug(f'test token to read from {label} InfluxDB'
                           f' by {query=} {params=}...')
        try:
            client.query_api().query(query=query, params=params)
        except ApiException as e:
            self._logger.exception(f'{label} client ApiException {e.reason=}'
                                   f' ({e.response.status=}): {e}')
            raise RuntimeError(e)

    def _raise_if_no_write_access(self, *, client: InfluxDBClient,
                                  config: InfluxConfig, label: str) -> None:
        test_point = (
            Point(self._test_metric_name).
            field(self._test_field_name, self._test_field_value).
            time(datetime.now())
        )
        self._logger.debug(f'test token to write {test_point} to {label}'
                           f' InfluxDB...')
        try:
            with client.write_api() as api:
                api.write(record=test_point,
                          bucket=config.bucket,
                          write_precision=self._write_precision)
        except ApiException as e:
            self._logger.exception(f'{label} client ApiException {e.reason=}'
                                   f' ({e.response.status=}): {e}')
            raise RuntimeError(e)

    def _get_most_recent_time(self, *, client: InfluxDBClient,
                              config: InfluxConfig, label: str,
                              remote: bool) -> Optional[datetime]:
        tags_filter = (
            ' |> filter(fn: (r) => r._measurement != test_metric_name)'
        )
        tags_filter += ' |> filter(fn: (r) => r.{} == "{}")'.format(
            self._REPLICATION_TAG,
            self._REPLICATION_VALUE
        )
        if self._additional_tags:
            tags_filter += ' |> filter(fn: (r) => {})'.format(
                ' and '.join((f'r.{tag_name} == "tag_{tag_name}_value"'
                              for tag_name in self._additional_tags.keys()))
            )
        query = ('from(bucket: bucket)'
                 ' |> range(start: duration(v: time_range))'
                 f'{tags_filter if remote else ""}'
                 ' |> group() |> last()')
        params = {'bucket': config.bucket,
                  'time_range': self._max_time_range}
        if remote:
            params['test_metric_name'] = self._test_metric_name
            for tag_name, tag_value in self._additional_tags.items():
                params[f'tag_{tag_name}_value'] = tag_value
        self._logger.debug(f'get most recent {label} InfluxDB time'
                           f' by {query=} {params=}...')
        results = client.query_api().query(query=query, params=params)
        try:
            result = results[0].records[0].get_time()
        except IndexError:
            result = None
        self._logger.debug(f'... {result=}')
        return result

    def _get_most_recent_remote_time(self) -> Optional[datetime]:
        return self._get_most_recent_time(client=self._remote_client,
                                          config=self._remote_config,
                                          label='remote', remote=True)

    def _get_most_recent_local_time(self) -> Optional[datetime]:
        return self._get_most_recent_time(client=self._local_client,
                                          config=self._local_config,
                                          label='local', remote=False)

    def _get_local_data_batch(self) -> list[Point]:
        query = (
            'from(bucket: bucket)'
            ' |> range(start: {})'
            ' |> group(columns: ["_time", "_measurement"])'
        ).format(
            'duration(v: start)'
            if self._last_synchronised_ts is None else
            'start'
        )
        params = {
            'bucket': self._local_config.bucket,
            'start': (self._max_time_range
                      if self._last_synchronised_ts is None else
                      int(self._last_synchronised_ts.timestamp()))
        }
        self._logger.debug(f'acquiring stream of data from local InfluxDB'
                           f' by {query=} {params=}...')
        api = self._local_client.query_api()
        result_stream = api.query_stream(query=query, params=params)
        data_batch = []
        for result in result_stream:
            time = result.get_time()
            if (self._last_synchronised_ts is not None
                    and time < self._last_synchronised_ts):
                continue
            measurement = result.get_measurement()
            field = result.get_field()
            value = result.get_value()
            point = Point(measurement).field(field, value).time(time)
            tags = result.values
            BUILTIN_FIELDS = ['result', 'table', '_start', '_stop', '_time',
                              '_value', '_field', '_measurement']
            for tag in filterfalse(lambda tag: tag in BUILTIN_FIELDS,
                                   tags.keys()):
                point = point.tag(tag, tags[tag])
            point = point.tag(self._REPLICATION_TAG, self._REPLICATION_VALUE)
            for tag_name, tag_value in self._additional_tags.items():
                point = point.tag(tag_name, tag_value)
            data_batch.append(point)
        result_stream.close()
        self._logger.debug(f'... got {len(data_batch)} points')
        return data_batch

    def _send_one_batch_and_update_cache(self) -> None:
        point_batch = self._get_local_data_batch()
        if not point_batch:
            self._logger.debug(f'{point_batch=}')
            return
        self._logger.info(f'writing data batch of {len(point_batch)}'
                          f' elements...')
        with self._remote_client.write_api() as api:
            api.write(record=point_batch,
                      bucket=self._remote_config.bucket,
                      write_precision=self._write_precision)
        self._logger.debug('... done')
        synced_ts = point_batch[-1]._time
        self._logger.debug(f'updating last sync timestamp to'
                           f' {synced_ts} ({synced_ts.timestamp()})')
        self._last_synchronised_ts = synced_ts
