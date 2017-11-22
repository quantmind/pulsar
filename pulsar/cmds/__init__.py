"""Useful distutils commands for continuous integration and deployment

These commands works in python 2 too
"""
from .test import Bench, Test
from .linux_wheels import ManyLinux
from .pypi_version import PyPi
from .s3data import S3Data


__all__ = [
    'Bench',
    'Test',
    'ManyLinux',
    'PyPi',
    'S3Data'
]
