# -*- coding: utf-8 -*-

import os

import txtarantool as tnt

from twisted.internet import base
from twisted.internet import defer
from twisted.trial import unittest

import config

base.DelayedCall.debug = False
tnt_sock = config.sock


class TestUnixConnectionMethods(unittest.TestCase):
    @defer.inlineCallbacks
    def test_UnixConnection(self):
        db = yield tnt.UnixConnection(tnt_sock, reconnect=False)
        self.assertIsInstance(db, tnt.UnixConnectionHandler)
        r = yield db.ping()
        self.assertEqual(r, "PING OK")
        yield db.disconnect()

    @defer.inlineCallbacks
    def test_UnixConnectionPool(self):
        db = yield tnt.UnixConnectionPool(tnt_sock, poolsize=2, reconnect=False)
        self.assertIsInstance(db, tnt.UnixConnectionHandler)
        r = yield db.ping()
        self.assertEqual(r, "PING OK")
        yield db.disconnect()

    @defer.inlineCallbacks
    def test_lazyUnixConnection(self):
        db = tnt.lazyUnixConnection(tnt_sock, reconnect=False)
        self.assertIsInstance(db._connected, defer.Deferred)
        db = yield db._connected
        self.assertIsInstance(db, tnt.UnixConnectionHandler)
        r = yield db.ping()
        self.assertEqual(r, "PING OK")
        yield db.disconnect()

    @defer.inlineCallbacks
    def test_lazyUnixConnectionPool(self):
        db = tnt.lazyUnixConnectionPool(tnt_sock, reconnect=False)
        self.assertIsInstance(db._connected, defer.Deferred)
        db = yield db._connected
        self.assertIsInstance(db, tnt.UnixConnectionHandler)
        r = yield db.ping()
        self.assertEqual(r, "PING OK")
        yield db.disconnect()


if not os.path.exists(tnt_sock):
    TestUnixConnectionMethods.skip = True
