#!/usr/bin/env python
# -*- coding: utf-8 -*-

import txtarantool as tnt

import time

from twisted.internet import defer
from twisted.internet import reactor


@defer.inlineCallbacks
def main():
    tc = yield tnt.Connection()
    print tc

    t0 = time.time()
    r = yield tc.ping()
    assert r == b'', "invalid ping response: %s" % (r)
    print "ping ok: %s" % (time.time() - t0)


if __name__ == "__main__":
    main().addCallback(lambda _: reactor.stop())
    reactor.run()
