#!/usr/bin/env python
from setuptools import setup

with open('README.md') as f:
    long_description = f.read()

setup(
    name='nameko-mongodb',
    version='1.1.1',
    description='Simple MongoDb dependency for nameko (microservices framework). '
                'Based on https://github.com/saiqi/nameko-mongodb',
    long_description=long_description,
    long_description_content_type='text/markdown',
    author='Alex Shinkevich',
    author_email='alex.shinkevich@gmail.com',
    url='https://github.com/alexshin/nameko-mongodb',
    packages=['nameko_mongodb'],
    install_requires=[
        'nameko>=2.11.0',
        'pymongo'
    ],
    extra_requires=[
        'pytest==3.3.0'
    ],
    zip_safe=True,
    license='Apache License, Version 2.0',
    classifiers=[
        "Programming Language :: Python",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Topic :: Internet",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Intended Audience :: Developers",
    ]
)