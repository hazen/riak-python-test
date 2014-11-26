#!/usr/bin/env python
import platform
from setuptools import setup, find_packages
from version import get_version
from commands import preconfigure, configure, create_bucket_types

install_requires = ["riak >=2.1.0", "python_twitter >= 2.0"]
requires = ["riak(>=2.1.0)", "python_twitter(>=2.0)"]
tests_require = []

setup(
    name='riak-python-test',
    version=get_version(),
    packages=find_packages(),
    requires=requires,
    install_requires=install_requires,
    tests_require=tests_require,
    package_data={'riak-python-test': ['erl_src/*']},
    description='Tester for Python client for Riak',
    zip_safe=True,
    options={'easy_install': {'allow_hosts': 'pypi.python.org'}},
    include_package_data=True,
    license='Apache 2',
    platforms='Platform Independent',
    author='Basho Technologies',
    author_email='clients@basho.com',
    test_suite='riak.tests.suite',
    url='https://github.com/javajolt/riak-python-test',
    cmdclass={'create_bucket_types': create_bucket_types,
              'preconfigure': preconfigure,
              'configure': configure},
    classifiers=['License :: OSI Approved :: Apache Software License',
                 'Intended Audience :: Developers',
                 'Operating System :: OS Independent',
                 'Topic :: Database']
    )
