txtarantool
===========

[![Build Status](https://travis-ci.org/zlobspb/txtarantool.png?branch=master)](http://travis-ci.org/zlobspb/txtarantool)

non-blocking [tarantool](https://github.com/mailru/tarantool) client for python [twisted](http://twistedmatrix.com/)

### Version 0.1 ###
Version 0.1 supports all of tarantool commands but it's work in progress...
You are more than welcome to join me.
Currently only insert, insert_ret, select and call are covered with tests.

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

However, if you really really insist in installing, use the following command:

	git clone --depth=1 --branch=master git://github.com/zlobspb/txtarantool.git txtarantool && sudo pip install ./txtarantool/ --use-mirrors && rm -rf ./txtarantool/

### Unit Tests ###

[Twisted Trial](http://twistedmatrix.com/trac/wiki/TwistedTrial) unit tests
are available. Just start tarantool, and run ``trial ./tests``.

### TODO ###
- tests for all tarantool commands
- update README.md with complete documentation and small usage examples
- examples
