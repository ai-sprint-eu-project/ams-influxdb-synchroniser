# coding: utf-8

from dataclasses import dataclass


@dataclass
class InfluxConfig:
    address: str
    token: str
    organisation: str
    bucket: str

    def __str__(self) -> str:
        return '{{URL={}, token={}, org={}, bucket={}}}'.format(
            self.address,
            '<redacted>',
            self.organisation,
            self.bucket
        )
