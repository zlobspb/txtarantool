#!/usr/bin/env python
# -*- coding: utf-8 -*-

#README.md example

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
