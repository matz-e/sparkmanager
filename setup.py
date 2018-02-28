"""Installation setup for bbp.spark
"""

from setuptools import setup

SPARKMANAGER_VERSION = '0.0.1'


def setup_package():
    """Provide setuptools configuration

    setup.cfg has more metadata.
    """
    setup(
        version=SPARKMANAGER_VERSION,
        packages=[
            'sparkmanager',
        ],
        install_requires=[
            'pyspark'
        ]
    )


if __name__ == '__main__':
    setup_package()
