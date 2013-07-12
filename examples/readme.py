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
