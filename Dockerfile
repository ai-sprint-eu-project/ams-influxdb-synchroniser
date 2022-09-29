# syntax=docker/dockerfile:1

FROM python:3.10 AS base
RUN pip --no-cache-dir install --upgrade pip
RUN mkdir /tmp/influx_sync
ADD pyproject.toml /tmp/influx_sync/
ADD src/ /tmp/influx_sync/src
RUN pip --no-cache-dir install --user /tmp/influx_sync/

FROM python:3.10-alpine
COPY --from=base /root/.local /root/.local
LABEL org.opencontainers.image.title="InfluxDB Synchroniser" \
    org.opencontainers.image.description="AI-SPRINT Monitoring Subsystem (AMS) InfluxDB Synchroniser" \
    org.opencontainers.image.version="1.0" \
    org.opencontainers.image.authors="Michał Soczewka" \
    org.opencontainers.image.licenses="MIT" \
    org.opencontainers.image.url="https://github.com/ai-sprint-eu-project/monitoring-subsystem" \
    org.opencontainers.image.documentation="https://github.com/ai-sprint-eu-project/monitoring-subsystem"
ENV PATH=/root/.local/bin:$PATH
CMD ["influx-synchroniser"]
