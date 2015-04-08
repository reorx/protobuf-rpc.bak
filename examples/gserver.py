#!/usr/bin/env python
# -*- coding: utf-8 -*-

from gevent.server import StreamServer


def handle(socket, address):
    print 'new connection!'


server = StreamServer(('127.0.0.1', 1234), handle)
server.serve_forever()
