from setuptools import setup, find_packages

PACKAGE = 'coinlibquoinex'

setup(
    name=PACKAGE,
    version='0.0.1',
    packages=find_packages(exclude=['tests']),
    url='https://github.com/tetocode',
    license='',
    author='tetocode',
    author_email='',
    description='',
    install_requires=['pyjwt', 'requests'],
    extras_require={
        'test': ['pytest'],
    },
)
