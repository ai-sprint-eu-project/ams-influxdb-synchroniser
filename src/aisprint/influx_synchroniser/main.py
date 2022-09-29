#!/usr/bin/env python
# coding: utf-8

import json
import logging
from os import environ
import sys

from .influx_config import InfluxConfig
from .sync_controller import SyncController

from ._config import (DEFAULT_INFLUX_RETENTION_POLICY,
                      DEFAULT_WRITE_PRECISION,
                      LOGGER_FORMAT, LOGGER_NAME, LOG_LEVEL)
from ._constants import (
    LOCAL_INFLUX_URL_ENV_VAR_NAME,
    DEFAULT_LOCAL_INFLUX_URL,
    LOCAL_INFLUX_TOKEN_ENV_VAR_NAME,
    LOCAL_INFLUX_ORG_ENV_VAR_NAME,
    DEFAULT_INFLUX_ORG,
    LOCAL_INFLUX_BUCKET_ENV_VAR_NAME,
    REMOTE_INFLUX_URL_ENV_VAR_NAME,
    REMOTE_INFLUX_TOKEN_ENV_VAR_NAME,
    REMOTE_INFLUX_ORG_ENV_VAR_NAME,
    REMOTE_INFLUX_BUCKET_ENV_VAR_NAME,
    ADDITIONAL_TAGS_ENV_VAR_NAME
)


def main():

    logging.basicConfig(format=LOGGER_FORMAT, level=LOG_LEVEL)

    logger = logging.getLogger(LOGGER_NAME)

    local_token = environ.get(LOCAL_INFLUX_TOKEN_ENV_VAR_NAME)
    if not local_token:
        msg: str = 'no local InfluxDB token'
        logger.critical(msg)
        raise RuntimeError(msg)
    local_bucket = environ.get(LOCAL_INFLUX_BUCKET_ENV_VAR_NAME)
    if not local_bucket:
        msg: str = 'no local InfluxDB bucket'
        logger.critical(msg)
        raise RuntimeError(msg)
    remote_url = environ.get(REMOTE_INFLUX_URL_ENV_VAR_NAME)
    if not remote_url:
        msg: str = 'no remote InfluxDB URL'
        logger.critical(msg)
        raise RuntimeError(msg)
    remote_token = environ.get(REMOTE_INFLUX_TOKEN_ENV_VAR_NAME)
    if not remote_token:
        msg: str = 'no remote InfluxDB token'
        logger.critical(msg)
        raise RuntimeError(msg)
    remote_bucket = environ.get(REMOTE_INFLUX_BUCKET_ENV_VAR_NAME,
                                local_bucket)
    tags = environ.get(ADDITIONAL_TAGS_ENV_VAR_NAME)
    try:
        tags = json.loads(tags) if tags else None
    except Exception as e:
        logger.critical(e)
        raise RuntimeError(e)

    controller = SyncController(
        local_config=InfluxConfig(
            address=environ.get(LOCAL_INFLUX_URL_ENV_VAR_NAME,
                                DEFAULT_LOCAL_INFLUX_URL),
            token=local_token,
            organisation=environ.get(LOCAL_INFLUX_ORG_ENV_VAR_NAME,
                                     DEFAULT_INFLUX_ORG),
            bucket=local_bucket
        ),
        global_config=InfluxConfig(
            address=remote_url,
            token=remote_token,
            organisation=environ.get(REMOTE_INFLUX_ORG_ENV_VAR_NAME,
                                     DEFAULT_INFLUX_ORG),
            bucket=remote_bucket
        ),
        max_time_range=DEFAULT_INFLUX_RETENTION_POLICY,
        write_precision=DEFAULT_WRITE_PRECISION,
        additional_tags=tags
    )

    try:
        controller.init()
    except (KeyboardInterrupt, RuntimeError) as e:
        logger.error(e)
        sys.exit(1)

    try:
        controller.run()
    except RuntimeError as e:
        logger.error(e)
        sys.exit(1)
    except KeyboardInterrupt:
        logger.info('keyboard interrupt')


if __name__ == '__main__':
    main()
