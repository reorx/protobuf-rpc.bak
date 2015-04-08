# Copyright (c) 2008 Alan Kligman
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
# 
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

import gevent.monkey
gevent.monkey.patch_all()

from test_pb2 import *
from protobufrpc.synchronous import TcpServer
import gevent

class TestService( Test ):
    def Echo( self, rpc_controller, request, done ):
        response = EchoResponse()
        response.text = request.text
        print 'gevent sleep 3'
        gevent.sleep(3)
        done( response )

    def Ping( self, rpc_controller, request, done ):
        response = PingResponse()
	done( response )

class MathService( Math ):
    def Add( self, rpc_controller, request, done ):
        response = MathResponse()
        response.result = request.first + request.second
        done( response )

    def Multiply( self, rpc_controller, request, done ):
        response = MathResponse()
        response.result = request.first * request.second
        done( response )

testService = TestService()
mathService = MathService()
server = TcpServer( ("localhost", 8080), testService, mathService )
server.serve_forever()
