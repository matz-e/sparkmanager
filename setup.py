"""Installation setup for bbp.spark
"""

from setuptools import setup

SPARKSETUP_VERSION = '0.0.1'


def setup_package():
    """Provide setuptools configuration

    setup.cfg has more metadata.
    """
    setup(
        version=SPARKSETUP_VERSION,
        packages=[
            'sparksetup',
        ],
        install_requires=[
            'pyspark'
        ]
    )


if __name__ == '__main__':
    setup_package()
