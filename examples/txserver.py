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

from test_pb2 import *
from protobufrpc.tx import Factory, Proxy
from twisted.internet.endpoints import TCP4ServerEndpoint
from twisted.internet import reactor


class TestService(Test):
    def Echo(self, rpc_controller, request, done):
        response = EchoResponse()
        response.text = request.text
        done(response)

    def Ping(self, rpc_controller, request, done):
        response = PingResponse()
        done(response)


class MathService(Math):
    def Add(self, rpc_controller, request, done):
        response = MathResponse()
        response.result = request.first + request.second
        done(response)

    def Multiply(self, rpc_controller, request, done):
        response = MathResponse()
        response.result = request.first * request.second
        done(response)


def rpc_error(fail):
    print "rpc error:", fail.getErrorMessage()


def print_response(response):
    print "response:", response


def send_request(protocol, proxy):
    request = EchoRequest()
    request.text = "Hello world!"
    echoed = proxy.Test.Echo(request)
    echoed.addCallback(print_response)
    echoed.addErrback(rpc_error)


def protocol_callback(protocol):
    proxy = Proxy(Test_Stub(protocol), Math_Stub(protocol))
    reactor.callLater(5, send_request, protocol, proxy)


testService = TestService()
mathService = MathService()
factory = Factory(protocol_callback, testService, mathService)
endpoint = TCP4ServerEndpoint(reactor, 8080)
endpoint.listen(factory)
reactor.run()
