#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys

import cyclone.web
import txtarantool
from twisted.internet import defer
from twisted.internet import reactor
from twisted.python import log

#README.md example

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
    tnt_conn = None

    @classmethod
    def setup(self):
        tarantoolMixin.tnt_conn = txtarantool.lazyConnectionPool()


class SetHandler(cyclone.web.RequestHandler, tarantoolMixin):
    @defer.inlineCallbacks
    def get(self, kv):
        try:
            key, value = kv.split('=')
        except:
            raise cyclone.web.HTTPError(400, "Bad parameters")

        try:
            value = yield self.tnt_conn.replace_ret(0, None, key, value)
        except Exception, e:
            log.msg("tarantool failed to replace('%s'): %s" % (key, str(e)))
            raise cyclone.web.HTTPError(503)

        self.set_header("Content-Type", "text/plain")
        self.write("set: %s=%s\r\n" % (key, value[0][1]))


class GetHandler(cyclone.web.RequestHandler, tarantoolMixin):
    @defer.inlineCallbacks
    def get(self, key):
        try:
            value = yield self.tnt_conn.select(0, 0, None, key)
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
            value = yield self.tnt_conn.delete_ret(0, None, key)
        except Exception, e:
            log.msg("tarantool failed to get('%s'): %s" % (key, str(e)))
            raise cyclone.web.HTTPError(503)

        self.set_header("Content-Type", "text/plain")
        if value:
            self.write("del: %s=%s\r\n" % (key, value[0][1]))
        else:
            self.write("del: no such key\r\n")


def main():
    log.startLogging(sys.stdout)
    reactor.listenTCP(8888, Application(), interface="127.0.0.1")
    reactor.run()


if __name__ == "__main__":
    main()
