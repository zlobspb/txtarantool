txtarantool
===========

[![Build Status](https://travis-ci.org/zlobspb/txtarantool.png?branch=master)](http://travis-ci.org/zlobspb/txtarantool)

non-blocking [tarantool](https://github.com/mailru/tarantool) client for python [twisted](http://twistedmatrix.com/)

### Features ###

- Connection Pools
- Lazy Connections
- Automatic Reconnection
- Unix Socket Connections

Install
-------

Bear in mind that ``txtarantool.py`` is pure-python, in a single file.
Thus, there's absolutely no need to install it. Instead, just copy it to your
project directory and start using.

Latest source code is at <https://github.com/zlobspb/txtarantool>.

However, if you really really insist in installing, use the following commands:

	git clone --depth=1 --branch=master git://github.com/zlobspb/txtarantool.git txtarantool
	sudo pip install ./txtarantool/ --use-mirrors
	rm -rf ./txtarantool/

### Unit Tests ###

[Twisted Trial](http://twistedmatrix.com/trac/wiki/TwistedTrial) unit tests
are available. Just start tarantool with [tarantool.cfg](https://raw.github.com/zlobspb/txtarantool/master/tarantool.cfg) config, and run ``trial ./tests``.

API description
---------------

txtarantool supports all tarantool server commands and provides following API to access it:

- `ping` - send ping packet to tarantool server and receive response with empty body
- `insert` - insert tuple, if primary key exists server will return error
- `insert_ret` - insert tuple, inserted tuple is sent back, if primary key exists server will return error
- `select` - select tuple(s)
- `select_ext` - select tuple(s), additional parameters are: offset and limit
- `update` - send update command(s)
- `update_ret` - send update command(s), updated tuple(s) is(are) sent back
- `delete` - delete tuple by primary key
- `delete_ret` - delete tuple by primary key, deleted tuple is sent back
- `replace` - insert tuple, if primary key exists it will be rewritten
- `replace_ret` - insert tuple, inserted tuple is sent back, if primary key exists it will be rewritten
- `replace_req` - insert tuple, if tuple with same primary key doesn't exist server will return error
- `replace_req_ret` - insert tuple, inserted tuple is sent back, if tuple with same primary key doesn't exist server will return error
- `call` - call server procedure

Usage
-----

First thing to do is choose what type of connection you want. The driver
supports single connection, connection pools and all of them can be *lazy*,
which is explained later (because I'm lazy now).

[Example](https://raw.github.com/zlobspb/txtarantool/master/examples/readme.py):
```python
#!/usr/bin/env python
# -*- coding: utf-8 -*-

# tarantool space configuration
# space[0].enabled = 1
# space[0].index[0].type = "HASH"
# space[0].index[0].unique = 1
# space[0].index[0].key_field[0].fieldno = 0
# space[0].index[0].key_field[0].type = "STR"

import txtarantool as tnt

from twisted.internet import defer
from twisted.internet import reactor


@defer.inlineCallbacks
def main():
    tc = yield tnt.Connection()
    print tc

    yield tc.replace(0, "foo", "bar", "baz", "quux")
    v = yield tc.select(0, 0, None, "foo")
    print v

    yield tc.disconnect()


if __name__ == "__main__":
    main().addCallback(lambda ign: reactor.stop())
    reactor.run()
```

Easily switch between ``tarantool.Connection()`` and ``tarantool.ConnectionPool()``
with absolutely no changes to the logic of your program.

These are all the supported methods for connecting to tarantool::
```python
    Connection(host, port, reconnect)
    lazyConnection(host, port, reconnect)

    ConnectionPool(host, port, poolsize, reconnect)
    lazyConnectionPool(host, port, poolsize, reconnect)

    UnixConnection(path, reconnect)
    lazyUnixConnection(path, reconnect)

    UnixConnectionPool(path, poolsize, reconnect)
    lazyUnixConnectionPool(path, poolsize, reconnect)
```

The arguments are:

- host: the IP address or hostname of the tarantool server. [default: localhost]
- port: port number of the tarantool server. [default: 33013]
- path: path of tarantool server's socket [default: /tmp/tarantool.sock]
- poolsize: how many connections to make. [default: 10]
- reconnect: auto-reconnect if connection is lost. [default: True]

### Connection Handlers ###

All connection methods return a connection handler object at some point.

Normal connections (not lazy) return a deferred, which is fired with the
connection handler after the connection is established.

In case of connection pools, it will only fire the callback after all
connections are set up, and ready.

Connection handler is the client interface with tarantool. It accepts all the
commands supported by tarantool, such as ``select``, ``insert``, etc.
It is the ``tc`` object in the example below.

Connection handlers will automatically select one of the available connections
in connection pools, and automatically reconnect to tarantool when necessary.

If the connection with tarantool is lost, all commands will raise the
``ConnectionError`` exception, to indicate that there's no active connection.
However, if the ``reconnect`` argument was set to ``True`` during the
initialization, it will continuosly try to reconnect, in background.

[Example](https://raw.github.com/zlobspb/txtarantool/master/examples/readme2.py):
```python
#!/usr/bin/env python
# -*- coding: utf-8 -*-

# tarantool space configuration
# space[0].enabled = 1
# space[0].index[0].type = "HASH"
# space[0].index[0].unique = 1
# space[0].index[0].key_field[0].fieldno = 0
# space[0].index[0].key_field[0].type = "STR"

import txtarantool as tnt

from twisted.internet import defer
from twisted.internet import reactor


def sleep(n):
    d = defer.Deferred()
    reactor.callLater(n, lambda *ign: d.callback(None))
    return d

@defer.inlineCallbacks
def main():
    tc = yield tnt.Connection()
    print tc

    yield tc.replace(0, "foo", "bar", "baz", "quux")

    # sleep, so you can kill tarantool
    print "sleeping for 5s, kill tarantool now..."
    yield sleep(5)

    try:
        v = yield tc.select(0, 0, None, "foo")
        print v

        yield tc.disconnect()
    except tnt.ConnectionError, e:
        print str(e)

if __name__ == "__main__":
    main().addCallback(lambda ign: reactor.stop())
    reactor.run()
```

### Lazy Connections ###

This type of connection will immediately return the connection handler object,
even before the connection is made.

It will start the connection, (or connections, in case of connection pools) in
background, and automatically reconnect if necessary.

You want lazy connections when you're writing servers, like web servers, or
any other type of server that should not wait for the tarantool connection during
the initialization of the program.

The example below is a web application, which will expose tarantool replace_ret, select and
delete_ret commands over HTTP.

If the database connection is down (either because tarantool is not running, or
whatever reason), the web application will start normally. If connection is
lost during the operation, nothing will change.

When there's no connection, all commands will fail, therefore the web
application will respond with HTTP 503 (Service Unavailable). It will resume to
normal once the connection with tarantool is re-established.

Try killing tarantool server after the application is running, and make a couple
of requests. Then, start tarantool again and give it another try.

[Example](https://raw.github.com/zlobspb/txtarantool/master/examples/readme3.py):
```python
#!/usr/bin/env python
# coding: utf-8

import sys

import cyclone.web
import txtarantool
from twisted.internet import defer
from twisted.internet import reactor
from twisted.python import log

# tarantool space configuration
# space[0].enabled = 1
# space[0].index[0].type = "HASH"
# space[0].index[0].unique = 1
# space[0].index[0].key_field[0].fieldno = 0
# space[0].index[0].key_field[0].type = "STR"


class Application(cyclone.web.Application):
    def __init__(self):
        handlers = [
            (r"/set/(.+)", SetHandler),
            (r"/get/(.+)", GetHandler),
            (r"/del/(.+)", DelHandler),
        ]

        tarantoolMixin.setup()
        cyclone.web.Application.__init__(self, handlers, debug=True)


class tarantoolMixin(object):
    tarantool_conn = None

    @classmethod
    def setup(self):
        tarantoolMixin.tarantool_conn = txtarantool.lazyConnectionPool()


class SetHandler(cyclone.web.RequestHandler, tarantoolMixin):
    @defer.inlineCallbacks
    def get(self, kv):
        try:
            key, value = kv.split('=')
        except:
            raise cyclone.web.HTTPError(400, "Bad parameters")

        try:
            value = yield self.tarantool_conn.replace_ret(0, None, key, value)
        except Exception, e:
            log.msg("tarantool failed to replace('%s'): %s" % (key, str(e)))
            raise cyclone.web.HTTPError(503)

        self.set_header("Content-Type", "text/plain")
        self.write("set: %s=%s\r\n" % (key, value[0][1]))


class GetHandler(cyclone.web.RequestHandler, tarantoolMixin):
    @defer.inlineCallbacks
    def get(self, key):
        try:
            value = yield self.tarantool_conn.select(0, 0, None, key)
        except Exception, e:
            log.msg("tarantool failed to get('%s'): %s" % (key, str(e)))
            raise cyclone.web.HTTPError(503)

        self.set_header("Content-Type", "text/plain")
        if value:
            self.write("get: %s=%s\r\n" % (key, value[0][1]))
        else:
            self.write("get: no such key\r\n")


class DelHandler(cyclone.web.RequestHandler, tarantoolMixin):
    @defer.inlineCallbacks
    def get(self, key):
        try:
            value = yield self.tarantool_conn.delete_ret(0, None, key)
        except Exception, e:
            log.msg("tarantool failed to get('%s'): %s" % (key, str(e)))
            raise cyclone.web.HTTPError(503)

        self.set_header("Content-Type", "text/plain")
        self.write("del: %s=%s\r\n" % (key, value[0][1]))


def main():
    log.startLogging(sys.stdout)
    reactor.listenTCP(8888, Application(), interface="127.0.0.1")
    reactor.run()


if __name__ == "__main__":
    main()
```

This is the server running in one terminal:

    $ ./examples/readme3.py
    2013-07-12 15:52:50+0400 Log opened.
    2013-07-12 15:52:50+0400 Starting factory <txtarantool.TarantoolFactory instance at 0x10f9a24d0>
    2013-07-12 15:52:50+0400 Application starting on 8888
    2013-07-12 15:52:50+0400 Starting factory <__main__.Application instance at 0x10f3ff488>
    2013-07-12 15:52:57+0400 [http] 200 GET /set/foo=bar (127.0.0.1) 3.10ms
    2013-07-12 15:52:57+0400 [http] 200 GET /get/foo (127.0.0.1) 1.71ms
    2013-07-12 15:52:57+0400 [http] 200 GET /del/foo (127.0.0.1) 1.56ms
    (killed tarantool-server)
    2013-07-12 15:53:12+0400 tarantool failed to get('foo'): Not connected
    2013-07-12 15:53:12+0400 [http] 503 GET /get/foo (127.0.0.1) 1.18ms

And these are the requests, from ``curl`` in another terminal.

Set:

    $ curl -D - "http://localhost:8888/set/foo=bar"
    HTTP/1.1 200 OK
    Content-Length: 14
    Content-Type: text/plain

    set: foo=bar

Get:

    $ curl -D - "http://localhost:8888/get/foo"
    HTTP/1.1 200 OK
    Content-Length: 14
    Content-Type: text/plain

    get: foo=bar

Delete:

    $ curl -D - "http://localhost:8888/del/foo"
    HTTP/1.1 200 OK
    Content-Length: 14
    Content-Type: text/plain

    del: foo=bar

When tarantool is not running:

    $ curl -D - "http://localhost:8888/get/foo"
    HTTP/1.1 503 Service Unavailable
    Content-Length: 89
    Content-Type: text/html; charset=UTF-8

    <html><title>503: Service Unavailable</title><body>503: Service Unavailable</body></html>

Please see tests/test_commands.py for additional api usage examples.

## Bugs and issues
Bug reports and pull requests are more than welcome.

### LICENSE
txtarantool is published under BSD license.
