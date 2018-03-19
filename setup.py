"""Installation setup for bbp.spark
"""

from setuptools import setup
from setuptools.command.test import test as TestCommand

import sys

SPARKMANAGER_NAME = 'sparkmanager'
SPARKMANAGER_VERSION = '0.0.3'


class PyTest(TestCommand):
    user_options = [("addopts=", None, "Arguments to pass to pytest")]

    def initialize_options(self):
        TestCommand.initialize_options(self)
        self.addopts = ""

    def run_tests(self):
        import pytest
        import shlex
        sys.exit(pytest.main(shlex.split(self.addopts)))


def setup_package():
    """Provide setuptools configuration

    setup.cfg has more metadata.
    """
    setup(
        name=SPARKMANAGER_NAME,
        version=SPARKMANAGER_VERSION,
        packages=[
            'sparkmanager',
        ],
        install_requires=[
            'pyspark',
            'six'
        ],
        tests_require=[
            'pytest',
            'pytest-cov'
        ],
        cmdclass={
            'test': PyTest
        },
        scripts=[
            'scripts/sm_cluster'
        ]
    )


if __name__ == '__main__':
    setup_package()
