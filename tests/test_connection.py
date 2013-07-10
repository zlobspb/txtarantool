# -*- coding: utf-8 -*-

import txtarantool as tnt

from twisted.internet import base
from twisted.internet import defer
from twisted.trial import unittest

import config

base.DelayedCall.debug = False
tnt_host = config.host
tnt_port = config.port


class TestConnectionMethods(unittest.TestCase):
    timeout = 10

    @defer.inlineCallbacks
    def test_Connection(self):
        db = yield tnt.Connection(tnt_host, tnt_port, reconnect=False)
        self.assertIsInstance(db, tnt.ConnectionHandler)
        r = yield db.ping()
        self.assertEqual(len(r), 0)
        self.assertEqual(repr(r), "ping ok")
        yield db.disconnect()

    @defer.inlineCallbacks
    def test_ConnectionPool(self):
        db = yield tnt.ConnectionPool(tnt_host, tnt_port, poolsize=2, reconnect=False)
        self.assertIsInstance(db, tnt.ConnectionHandler)
        r = yield db.ping()
        self.assertEqual(len(r), 0)
        self.assertEqual(repr(r), "ping ok")
        yield db.disconnect()

    @defer.inlineCallbacks
    def test_lazyConnection(self):
        db = tnt.lazyConnection(tnt_host, tnt_port, reconnect=False)
        self.assertIsInstance(db._connected, defer.Deferred)
        db = yield db._connected
        self.assertIsInstance(db, tnt.ConnectionHandler)
        r = yield db.ping()
        self.assertEqual(len(r), 0)
        self.assertEqual(repr(r), "ping ok")
        yield db.disconnect()

    @defer.inlineCallbacks
    def test_lazyConnectionPool(self):
        db = tnt.lazyConnectionPool(tnt_host, tnt_port, reconnect=False)
        self.assertIsInstance(db._connected, defer.Deferred)
        db = yield db._connected
        self.assertIsInstance(db, tnt.ConnectionHandler)
        r = yield db.ping()
        self.assertEqual(len(r), 0)
        self.assertEqual(repr(r), "ping ok")
        yield db.disconnect()
