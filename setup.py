#!/usr/bin/env python

from os.path import realpath
from setuptools import setup, find_packages


def get_reqs_from_file(file):
    file_path = realpath(file)
    return [req for req in open(file_path, 'r').readlines()]


setup(
    name='gdax',
    version='1.0.6',
    author='Daniel Paquin',
    author_email='dpaq34@gmail.com',
    license='MIT',
    url='https://github.com/danpaquin/gdax-python',
    packages=find_packages(),
    install_requires=get_reqs_from_file('./requirements.txt'),
    description='The unofficial Python client for the GDAX API',
    download_url='https://github.com/danpaquin/gdax-Python/archive/master.zip',
    keywords=['gdax', 'gdax-api', 'orderbook', 'trade', 'bitcoin', 'ethereum', 'BTC', 'ETH', 'client', 'api', 'wrapper', 'exchange', 'crypto', 'currency', 'trading', 'trading-api', 'coinbase'],
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Intended Audience :: Financial and Insurance Industry',
        'Intended Audience :: Information Technology',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
    ],
)
