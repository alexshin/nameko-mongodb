# Overview

[![Build Status](https://travis-ci.org/alexshin/nameko-mongodb.svg?branch=master)](https://travis-ci.org/alexshin/nameko-mongodb)

This is DependencyProvider for [Nameko microservices framework](https://www.nameko.io) which
enables users to work with MongoDb.

First of all I should say that this package based on https://github.com/saiqi/nameko-mongodb . Thank you @saiqi :)

What has been changed:

* Automatic uploading to PyPI by Travis-CI (it was a primary purpose for building of my pet projects)
* Disabled by default results logging
* Some fixes to make this stuff work with new Nameko

## Requirements

* Python 2.7 / 3.4 / 3.5 / 3.6 / 3.7
* Nameko 3.11+
* MongoDb :)

## Installation

The same as you guessing: `pip install nameko-mongodb`

## How to use

There are some configuration options to use this package (required are in bold):

* **MONGODB_CONNECTION_URL** - connection URL
* MONGODB_DB_NAME - database name. Default is your service name
* MONGODB_USER - if you need to be authenticated, provide username
* MONGODB_PASSWORD
* MONGODB_AUTHENTICATION_BASE - a source to authenticate. See more information in [PyMongo documentation](http://api.mongodb.com/python/current/examples/authentication.html)
* MONGODB_AUTH_MECHANISM - see more information in [PyMongo documentation](http://api.mongodb.com/python/current/examples/authentication.html)

You can use the connection following way:

```python
from nameko.rpc import rpc
import MongoDatabase from nameko_mongodb


class YourService(object):
    name = 'your_service'

    database = MongoDatabase()

    @rpc
    def find_item(self):
        return self.database.your_collection.find_one()

```

Also this package can log all executions to `logging` collection. If you want to use it:

```python
from nameko.rpc import rpc
import MongoDatabase from nameko_mongodb


class YourService(object):
    name = 'your_service'

    database = MongoDatabase(result_backend=True)

    @rpc
    def find_item(self):
        return self.database.your_collection.find_one()

```

Sometimes there can be useful to get possibility executing callbacks for DependencyProvider methods (i.e. migrating 
data when dependency initialized etc.). There are some callbacks:

* `on_before_setup`: (instance)
* `on_after_setup`: (instance)
* `on_before_stop`: (instance)
* `on_after_stop`: (instance)
* `on_before_worker_setup`: (instance, worker_ctx)
* `on_after_worker_setup`: (instance, worker_ctx)
* `on_before_worker_result`: (instance, worker_ctx, result, exc_info)
* `on_after_worker_result`: (instance, worker_ctx, result, exc_info)

How to use callbacks:

```python
from nameko.rpc import rpc
import MongoDatabase from nameko_mongodb


class YourService(object):
    name = 'your_service'

    database = MongoDatabase(
        on_before_setup=lambda x: print("Start setup DP"), 
        on_after_setup=lambda x: print("Stop setup DP"),
    )

    @rpc
    def find_item(self):
        return self.database.your_collection.find_one()

```



## Contribution

I'd be glad to see your pull requests