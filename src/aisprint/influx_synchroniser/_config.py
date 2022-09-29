# coding: utf-8

from logging import INFO


DEFAULT_INFLUX_RETENTION_POLICY: str = '-24h'

DEFAULT_WRITE_PRECISION: str = 'ms'

LOGGER_FORMAT: str = ('%(asctime)s    [%(levelname)8s]'
                      '    [%(name){name_padding}s]:   '
                      '%(message)s'.format(name_padding=''))
LOGGER_NAME: str = 'infux_synchroniser'
LOG_LEVEL: int = INFO
