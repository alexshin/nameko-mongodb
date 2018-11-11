#!/usr/bin/env python
from setuptools import setup

setup(
    name='nameko-mongodb',
    version='0.0.2-dev1',
    description='Simple MongoDb dependency for nameko microservices',
    author='saiqi',
    author_email='julien.bernard.iphone@gamil.com',
    url='http://github.com/saiqi/nameko-mongodb',
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
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "Topic :: Internet",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Intended Audience :: Developers",
    ]
)