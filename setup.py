"""Installation setup for bbp.spark
"""

from setuptools import setup

SPARKMANAGER_NAME = 'sparkmanager'
SPARKMANAGER_VERSION = '0.0.4'


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
    setup_requires=[
        'pytest-runner'
    ],
    tests_require=[
        'pytest',
        'pytest-cov'
    ],
    scripts=[
        'scripts/sm_cluster'
    ]
)
