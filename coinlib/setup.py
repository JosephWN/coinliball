from setuptools import setup, find_packages

PACKAGE = 'coinlib'

setup(
    name=PACKAGE,
    version='0.0.1',
    packages=find_packages(exclude=['tests']),
    url='',
    license='',
    author='tetocode',
    author_email='',
    description='',
    install_requires=['requests', 'pubnub', 'pyyaml', 'websocket-client'],
    extras_require={
        'test': ['pytest'],
    },
)
