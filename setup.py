#!/usr/bin/env python
from setuptools import setup

setup(
    name='nameko-mongodb',
    version='0.0.2-dev',
    description='Simple MongoDb dependency for nameko (microservices framework). Based on https://github.com/saiqi/nameko-mongodb',
    author='Alex Shinkevich',
    author_email='alex.shinkevich@gmail.com',
    url='https://github.com/alexshin/nameko-mongodb',
    packages=['nameko_mongodb'],
    install_requires=[
        'nameko>=2.11.0',
        'pymongo'
    ],
    extra_requires=[
        'pytest==3.0.6'
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