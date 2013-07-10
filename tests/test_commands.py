# -*- coding: utf-8 -*-

import txtarantool as tnt

from twisted.internet import base
from twisted.internet import defer
from twisted.trial import unittest
from random import randint, choice

import config

base.DelayedCall.debug = False
tnt_host = config.host
tnt_port = config.port

space_no0 = config.space_no0
space_no1 = config.space_no1

insert_string_length_max = config.insert_string_length_max

insert_string_choice = config.insert_string_choice


class TestInsert(unittest.TestCase):

    @defer.inlineCallbacks
    def tearDown(self):
        db = yield tnt.Connection(tnt_host, tnt_port, reconnect=False)
        yield db.call(space_no0, "tear_down_space", None, space_no0)
        yield db.call(space_no0, "tear_down_space", None, space_no1)
        yield db.disconnect()

    @defer.inlineCallbacks
    def test_insert(self):
        db = yield tnt.Connection(tnt_host, tnt_port, reconnect=False)
        data = (
            (
                ''.join(choice(insert_string_choice) for x in xrange(randint(0, insert_string_length_max))),
                randint(0, 2 ** 32 - 1),
                long(randint(0, 2 ** 64 - 1)),
            ),
            (
                randint(0, 2 ** 32 - 1),
                ''.join(choice(insert_string_choice) for x in xrange(randint(0, insert_string_length_max))),
                long(randint(0, 2 ** 64 - 1)),
            ),
            (
                long(randint(0, 2 ** 64 - 1)),
                randint(0, 2 ** 32 - 1),
                ''.join(choice(insert_string_choice) for x in xrange(randint(0, insert_string_length_max))),
            ),
        )

        for t in data:
            r = yield db.insert(space_no0, *t)
            self.assertEqual(len(r), 0)
            self.assertIn(" inserted", repr(r))

        yield db.disconnect()


    @defer.inlineCallbacks
    def test_insert_list(self):
        db = yield tnt.Connection(tnt_host, tnt_port, reconnect=False)
        data = (
            (
                ''.join(choice(insert_string_choice) for x in xrange(randint(0, insert_string_length_max))),
                randint(0, 2 ** 32 - 1),
                long(randint(0, 2 ** 64 - 1)),
            ),
            (
                randint(0, 2 ** 32 - 1),
                ''.join(choice(insert_string_choice) for x in xrange(randint(0, insert_string_length_max))),
                long(randint(0, 2 ** 64 - 1)),
            ),
            (
                long(randint(0, 2 ** 64 - 1)),
                randint(0, 2 ** 32 - 1),
                ''.join(choice(insert_string_choice) for x in xrange(randint(0, insert_string_length_max))),
            ),
        )

        defer_list = []
        for t in data:
            r = db.insert(space_no0, *t)
            defer_list.append(r)

        result = yield defer.DeferredList(defer_list)
        for status, r in result:
            self.assertTrue(status)
            self.assertEqual(len(r), 0)
            self.assertIn(" inserted", repr(r))

        yield db.disconnect()

    @defer.inlineCallbacks
    def test_insert_ret(self):
        db = yield tnt.Connection(tnt_host, tnt_port, reconnect=False)
        data = (
            (
                ''.join(choice(insert_string_choice) for x in xrange(randint(0, insert_string_length_max))),
                randint(0, 2 ** 32 - 1),
                long(randint(0, 2 ** 64 - 1)),
            ),
            (
                randint(0, 2 ** 32 - 1),
                ''.join(choice(insert_string_choice) for x in xrange(randint(0, insert_string_length_max))),
                long(randint(0, 2 ** 64 - 1)),
            ),
            (
                long(randint(0, 2 ** 64 - 1)),
                randint(0, 2 ** 32 - 1),
                ''.join(choice(insert_string_choice) for x in xrange(randint(0, insert_string_length_max))),
            ),
        )

        for t in data:
            r = yield db.insert_ret(space_no0, tuple(type(x) for x in t), *t)
            self.assertEqual(len(r), 1)
            self.assertEqual(t, r[0])

        yield db.disconnect()


    @defer.inlineCallbacks
    def test_insert_ret_list(self):
        db = yield tnt.Connection(tnt_host, tnt_port, reconnect=False)
        data = (
            (
                ''.join(choice(insert_string_choice) for x in xrange(randint(0, insert_string_length_max))),
                randint(0, 2 ** 32 - 1),
                long(randint(0, 2 ** 64 - 1)),
            ),
            (
                randint(0, 2 ** 32 - 1),
                ''.join(choice(insert_string_choice) for x in xrange(randint(0, insert_string_length_max))),
                long(randint(0, 2 ** 64 - 1)),
            ),
            (
                long(randint(0, 2 ** 64 - 1)),
                randint(0, 2 ** 32 - 1),
                ''.join(choice(insert_string_choice) for x in xrange(randint(0, insert_string_length_max))),
            ),
        )

        defer_list = []
        for t in data:
            r = db.insert_ret(space_no0, tuple(type(x) for x in t), *t)
            defer_list.append(r)

        result = yield defer.DeferredList(defer_list)
        for i, r in enumerate(result):
            self.assertTrue(r[0])
            self.assertEqual(len(r[1]), 1)
            self.assertEqual(data[i], r[1][0])

        yield db.disconnect()


class TestSelect(unittest.TestCase):

    @defer.inlineCallbacks
    def test_select(self):
        db = yield tnt.Connection(tnt_host, tnt_port, reconnect=False)
        data = (
            (
                ''.join(choice(insert_string_choice) for x in xrange(randint(0, insert_string_length_max))),
                randint(0, 2 ** 32 - 1),
                long(randint(0, 2 ** 64 - 1)),
            ),
            (
                randint(0, 2 ** 32 - 1),
                ''.join(choice(insert_string_choice) for x in xrange(randint(0, insert_string_length_max))),
                long(randint(0, 2 ** 64 - 1)),
            ),
            (
                long(randint(0, 2 ** 64 - 1)),
                randint(0, 2 ** 32 - 1),
                ''.join(choice(insert_string_choice) for x in xrange(randint(0, insert_string_length_max))),
            ),
        )

        for t in data:
            r = yield db.insert(space_no0, *t)
            self.assertEqual(len(r), 0)
            self.assertIn(" inserted", repr(r))

        for t in data:
            r = yield db.select(space_no0, 0, 0, 0xffffffff, tuple(type(x) for x in t), t[0])
            self.assertEqual(len(r), 1)
            self.assertEqual(t, r[0])

        yield db.disconnect()


    @defer.inlineCallbacks
    def test_select_list(self):
        db = yield tnt.Connection(tnt_host, tnt_port, reconnect=False)
        data = (
            (
                ''.join(choice(insert_string_choice) for x in xrange(randint(0, insert_string_length_max))),
                randint(0, 2 ** 32 - 1),
                long(randint(0, 2 ** 64 - 1)),
            ),
            (
                randint(0, 2 ** 32 - 1),
                ''.join(choice(insert_string_choice) for x in xrange(randint(0, insert_string_length_max))),
                long(randint(0, 2 ** 64 - 1)),
            ),
            (
                long(randint(0, 2 ** 64 - 1)),
                randint(0, 2 ** 32 - 1),
                ''.join(choice(insert_string_choice) for x in xrange(randint(0, insert_string_length_max))),
            ),
        )

        defer_list = []
        for t in data:
            r = db.insert(space_no0, *t)
            defer_list.append(r)

        yield defer.DeferredList(defer_list)

        defer_list = []
        for t in data:
            r = db.select(space_no0, 0, 0, 0xffffffff, tuple(type(x) for x in t), t[0])
            defer_list.append(r)

        result = yield defer.DeferredList(defer_list)
        for i, r in enumerate(result):
            self.assertTrue(r[0])
            self.assertEqual(len(r[1]), 1)
            self.assertEqual(data[i], r[1][0])

        yield db.disconnect()

    @defer.inlineCallbacks
    def test_select_multi_field_index(self):
        db = yield tnt.Connection(tnt_host, tnt_port, reconnect=False)
        same_field = 3
        data = [
            (randint(0, 2 ** 32 - 1), same_field, 5),
            (randint(0, 2 ** 32 - 1), same_field, 7),
            (randint(0, 2 ** 32 - 1), same_field, 11),
        ]

        for t in data:
            r = yield db.insert(space_no1, *t)
            self.assertEqual(len(r), 0)
            self.assertIn(" inserted", repr(r))

        types = (int, int, int)
        r = yield db.select(space_no1, 1, 0, 0xffffffff, types, same_field)
        self.assertEqual(len(r), len(data))
        self.assertEqual(r, data)

        for t in data:
            r = yield db.select(space_no1, 1, 0, 0xffffffff, types, t[1], t[2])
            self.assertEqual(len(r), 1)
            self.assertEqual(r[0], t)

        yield db.disconnect()
