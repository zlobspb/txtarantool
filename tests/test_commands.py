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
        d1 = db.call("tear_down_space", None, str(space_no0))
        d2 = db.call("tear_down_space", None, str(space_no1))
        yield defer.DeferredList([d1, d2])
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
    def tearDown(self):
        db = yield tnt.Connection(tnt_host, tnt_port, reconnect=False)
        d1 = db.call("tear_down_space", None, str(space_no0))
        d2 = db.call("tear_down_space", None, str(space_no1))
        yield defer.DeferredList([d1, d2])
        yield db.disconnect()

    @defer.inlineCallbacks
    def test_select_ext(self):
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
            r = yield db.select_ext(space_no0, 0, 0, 0xffffffff, tuple(type(x) for x in t), t[0])
            self.assertEqual(len(r), 1)
            self.assertEqual(t, r[0])

        yield db.disconnect()

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
            r = yield db.select(space_no0, 0, tuple(type(x) for x in t), t[0])
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
            r = db.select(space_no0, 0, tuple(type(x) for x in t), t[0])
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
        identical_field = 3
        data = [
            (randint(0, 2 ** 32 - 1), identical_field, 5),
            (randint(0, 2 ** 32 - 1), identical_field, 7),
            (randint(0, 2 ** 32 - 1), identical_field, 11),
        ]

        for t in data:
            r = yield db.insert(space_no1, *t)
            self.assertEqual(len(r), 0)
            self.assertIn(" inserted", repr(r))

        types = (int, int, int)
        r = yield db.select(space_no1, 1, types, identical_field)
        self.assertEqual(len(r), len(data))
        self.assertEqual(r, data)

        for t in data:
            r = yield db.select(space_no1, 1, types, t[1], t[2])
            self.assertEqual(len(r), 1)
            self.assertEqual(r[0], t)

        yield db.disconnect()


class TestUpdate(unittest.TestCase):

    @defer.inlineCallbacks
    def tearDown(self):
        db = yield tnt.Connection(tnt_host, tnt_port, reconnect=False)
        d1 = db.call("tear_down_space", None, str(space_no0))
        d2 = db.call("tear_down_space", None, str(space_no1))
        yield defer.DeferredList([d1, d2])
        yield db.disconnect()

    @defer.inlineCallbacks
    def test_update(self):
        db = yield tnt.Connection(tnt_host, tnt_port, reconnect=False)

        yield db.insert(space_no0, 0, "hello world")

        commands = [
            [(1, "=", 1)],
            [(1, "+", 1)],
            [(1, "+", 1)] * 2,
            [(1, "!", "Bienvenue tout le monde!")],
            [(1, "#", "")],
        ]

        for c in commands:
            r = yield db.update(space_no0, (0,), c)
            self.assertEqual(len(r), 0)
            self.assertIn(" updated", repr(r))

        yield db.disconnect()

    @defer.inlineCallbacks
    def test_update_ret(self):
        db = yield tnt.Connection(tnt_host, tnt_port, reconnect=False)

        yield db.insert(space_no0, 0, "hello world")

        commands_field_result = [
            ([(1, "=", 1)], (int, int), [(0, 1)]),
            ([(1, "+", 1)], (int, int), [(0, 2)]),
            ([(1, "+", 1)] * 2, (int, int), [(0, 4)]),
            ([(1, "!", "Bienvenue tout le monde!")], (int, str, int), [(0, "Bienvenue tout le monde!", 4)]),
            ([(1, "#", "")], (int, int), [(0, 4)]),
        ]

        for c, f, t in commands_field_result:
            r = yield db.update_ret(space_no0, f, (0,), c)
            self.assertEqual(len(r), 1)
            self.assertEqual(t, r)

        yield db.disconnect()


class TestDelete(unittest.TestCase):

    @defer.inlineCallbacks
    def tearDown(self):
        db = yield tnt.Connection(tnt_host, tnt_port, reconnect=False)
        d1 = db.call("tear_down_space", None, str(space_no0))
        d2 = db.call("tear_down_space", None, str(space_no1))
        yield defer.DeferredList([d1, d2])
        yield db.disconnect()

    @defer.inlineCallbacks
    def test_delete(self):
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
            r = yield db.delete(space_no0, t[0])
            self.assertEqual(len(r), 0)
            self.assertIn(" deleted", repr(r))

        yield db.disconnect()

    @defer.inlineCallbacks
    def test_delete_list(self):
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

        defer_list = []
        for t in data:
            r = db.delete(space_no0, t[0])
            defer_list.append(r)

        result = yield defer.DeferredList(defer_list)
        for status, r in result:
            self.assertTrue(status)
            self.assertEqual(len(r), 0)
            self.assertIn(" deleted", repr(r))

        yield db.disconnect()

    @defer.inlineCallbacks
    def test_delete_ret(self):
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
            r = yield db.delete_ret(space_no0, tuple(type(x) for x in t), t[0])
            self.assertEqual(len(r), 1)
            self.assertEqual(t, r[0])

        yield db.disconnect()

    @defer.inlineCallbacks
    def test_delete_ret_list(self):
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

        defer_list = []
        for t in data:
            r = db.delete_ret(space_no0, tuple(type(x) for x in t), t[0])
            defer_list.append(r)

        result = yield defer.DeferredList(defer_list)
        for i, r in enumerate(result):
            self.assertTrue(r[0])
            self.assertEqual(len(r[1]), 1)
            self.assertEqual(data[i], r[1][0])

        yield db.disconnect()


class TestReplace(unittest.TestCase):

    @defer.inlineCallbacks
    def tearDown(self):
        db = yield tnt.Connection(tnt_host, tnt_port, reconnect=False)
        d1 = db.call("tear_down_space", None, str(space_no0))
        d2 = db.call("tear_down_space", None, str(space_no1))
        yield defer.DeferredList([d1, d2])
        yield db.disconnect()

    @defer.inlineCallbacks
    def test_replace(self):
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
            r = yield db.replace(space_no0, *t)
            self.assertEqual(len(r), 0)
            self.assertIn(" inserted", repr(r))

        yield db.disconnect()

    @defer.inlineCallbacks
    def test_replace_ret(self):
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

        for t in data:
            r = yield db.replace_ret(space_no0, tuple(type(x) for x in t), *t)
            self.assertEqual(len(r), 1)
            self.assertEqual(t, r[0])

        yield db.disconnect()

    @defer.inlineCallbacks
    def test_replace_req(self):
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
            try:
                r = yield db.replace_req(space_no0, *t)
            except Exception as e:
                self.assertIsInstance(e, tnt.TarantoolError)

        for t in data:
            r = yield db.insert_ret(space_no0, tuple(type(x) for x in t), *t)
            self.assertEqual(len(r), 1)
            self.assertEqual(t, r[0])

        for t in data:
            r = yield db.replace_req(space_no0, *t)
            self.assertEqual(len(r), 0)
            self.assertIn(" inserted", repr(r))

        yield db.disconnect()

    @defer.inlineCallbacks
    def test_replace_req_ret(self):
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
            try:
                r = yield db.replace_req_ret(space_no0, None, *t)
            except Exception as e:
                self.assertIsInstance(e, tnt.TarantoolError)

        for t in data:
            r = yield db.insert_ret(space_no0, tuple(type(x) for x in t), *t)
            self.assertEqual(len(r), 1)
            self.assertEqual(t, r[0])

        for t in data:
            r = yield db.replace_req_ret(space_no0, tuple(type(x) for x in t), *t)
            self.assertEqual(len(r), 1)
            self.assertEqual(t, r[0])

        yield db.disconnect()


class TestCall(unittest.TestCase):

    @defer.inlineCallbacks
    def tearDown(self):
        db = yield tnt.Connection(tnt_host, tnt_port, reconnect=False)
        d1 = db.call("tear_down_space", None, str(space_no0))
        d2 = db.call("tear_down_space", None, str(space_no1))
        yield defer.DeferredList([d1, d2])
        yield db.disconnect()

    @defer.inlineCallbacks
    def test_call(self):
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
            r = yield db.call("box.insert", tuple(type(x) for x in t), str(space_no0), *t)
            self.assertEqual(len(r), 1)
            self.assertEqual(t, r[0])

        for t in data:
            r = yield db.call("box.select", tuple(type(x) for x in t), str(space_no0), "0", t[0])
            self.assertEqual(len(r), 1)
            self.assertEqual(t, r[0])

        yield db.disconnect()
