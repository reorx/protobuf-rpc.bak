# Copyright (c) 2008 Alan Kligman
# Copyright (c) 2008 Johan Euphrosine
# Copyright (c) 2008 Eric Evans <eevans@racklabs.com>
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
from twisted.internet.address import _IPAddress

import twisted.internet.protocol
from twisted.internet.defer import Deferred
from twisted.protocols.basic import Int32StringReceiver
from twisted.internet.protocol import DatagramProtocol
from twisted.internet.protocol import connectionDone
from twisted.protocols.policies import ProtocolWrapper
import google.protobuf.service
from twisted.python.failure import Failure
from protobufrpc_pb2 import Rpc, Request, Response, Error
from common import Controller

__all__ = ["TcpChannel", "UdpChannel", "Proxy", "Factory"]


class RPCException(Exception):
    pass


class BaseChannel(google.protobuf.service.RpcChannel):
    id = 0

    def __init__(self):
        google.protobuf.service.RpcChannel.__init__(self)
        self._pending = {}
        self._services = {}

    def add_service(self, service):
        self._services[service.GetDescriptor().name] = service

    def unserialize_response(self, serializedResponse, responseClass,
                             rpcController):
        print "unserialize_response"
        response = responseClass()
        if serializedResponse.HasField('error'):
            #rpcController.setFailed(serializedResponse.error.text)
            raise RPCException(serializedResponse.error.text)
        else:
            response.ParseFromString(serializedResponse.serialized_response)

        return response

    def serialize_response(self, response, serializedRequest, controller):
        serializedResponse = Response()
        serializedResponse.id = serializedRequest.id

        if controller.Failed():
            serializedResponse.error.code = 1
            serializedResponse.error.text = controller.ErrorText()
        else:
            serializedResponse.serialized_response = response.SerializeToString()

        return serializedResponse

    def serialize_rpc(self, serializedResponse):
        rpc = Rpc()
        rpcResponse = rpc.response.add()
        rpcResponse.serialized_response = serializedResponse.serialized_response
        rpcResponse.id = serializedResponse.id
        if serializedResponse.error.code != 0:
            rpcResponse.error.code = serializedResponse.error.code
            rpcResponse.error.text = serializedResponse.error.text
        return rpc

    def _call_method(self, methodDescriptor, rpcController, request,
                     responseClass, done):
        self.id += 1
        d = Deferred()
        d.addCallback(self.unserialize_response, responseClass, rpcController)
        d.chainDeferred(done)
        self._pending[self.id] = d
        rpc = Rpc()
        rpcRequest = rpc.request.add()
        rpcRequest.method = methodDescriptor.containing_service.name + '.' + methodDescriptor.name
        rpcRequest.serialized_request = request.SerializeToString()
        rpcRequest.id = self.id
        return rpc

    def CallMethod(self, methodDescriptor, rpcController, request,
                   responseClass, done):
        # This method must be overridden.
        pass


class RpcErrors:
    SUCCESS = 0
    UNSERIALIZE_RPC = 1
    SERVICE_NOT_FOUND = 2
    METHOD_NOT_FOUND = 3
    CANNOT_DESERIALIZE_REQUEST = 4
    METHOD_ERROR = 5

    msgs = ['Success', 'Error when unserializing Rpc message',
            'Service not found', 'Method not found',
            'Cannot deserialized request', 'Method Error']


class TcpChannel(BaseChannel, Int32StringReceiver):
    def __init__(self, lost_cb=None):
        """
        :param (_IPAddress, Failure) -> None lost_cb: Tcp connection lost cb func
        :return:
        """
        self._lost_cb = lost_cb
        super(TcpChannel, self).__init__()

    def CallMethod(self, methodDescriptor, rpcController, request,
                   responseClass, done):
        rpc = self._call_method(methodDescriptor, rpcController, request,
                                responseClass, done)
        self.sendString(rpc.SerializeToString())

    def stringReceived(self, data):
        rpc = Rpc()
        rpc.ParseFromString(data)

        for serializedRequest in rpc.request:
            service = self._services[serializedRequest.method.split('.')[0]]
            if not service:
                self.sendError(serializedRequest.id,
                               RpcErrors.SERVICE_NOT_FOUND)
                return

            method = service.GetDescriptor().FindMethodByName(
                serializedRequest.method.split('.')[1])
            if not method:
                self.sendError(serializedRequest.id, RpcErrors.METHOD_NOT_FOUND)
                return

            request = service.GetRequestClass(method)()
            request.ParseFromString(serializedRequest.serialized_request)
            controller = Controller(peer=self.transport.getPeer())
            d = Deferred()
            d.addCallback(self.serialize_response, serializedRequest,
                          controller)
            d.addCallback(self.serialize_rpc)
            d.addCallback(lambda rpc: self.sendString(rpc.SerializeToString()))
            service.CallMethod(method, controller, request, d.callback)

        for serializedResponse in rpc.response:
            _id = serializedResponse.id
            if _id in self._pending:
                self._pending[_id].callback(serializedResponse)

    def sendError(self, id, code):
        rpc = Rpc()
        rpcResponse = rpc.response.add()
        rpcResponse.id = id
        rpcResponse.error.code = code
        rpcResponse.error.text = RpcErrors.msgs[code]
        self.sendString(rpc.SerializeToString())

    def connectionLost(self, reason=connectionDone):
        if self._lost_cb is not None:
            addr = self.transport.getPeer()
            self._lost_cb(addr, reason)


class UdpChannel(BaseChannel, DatagramProtocol):
    def __init__(self, host=None, port=None):
        self._host = host
        self._port = port
        self.connected = False
        BaseChannel.__init__(self)

    def startProtocol(self):
        if self._host and self._port:
            self.transport.connect(self._host, self._port)
            self.connected = True

    def datagramReceived(self, data, (host, port)):
        rpc = Rpc()
        rpc.ParseFromString(data)
        for serializedRequest in rpc.request:
            service = self._services[serializedRequest.method.split('.')[0]]

            if not service:
                self.sendError(serializedRequest.id,
                               RpcErrors.SERVICE_NOT_FOUND, host, port)
                return

            method = service.GetDescriptor().FindMethodByName(
                serializedRequest.method.split('.')[1])
            if not method:
                self.sendError(serializedRequest.id,
                               RpcErrors.METHOD_NOT_FOUND, host, port)
                return

            request = service.GetRequestClass(method)()
            request.ParseFromString(serializedRequest.serialized_request)
            controller = Controller()
            d = Deferred()
            d.addCallback(self.serialize_response, serializedRequest)
            d.addCallback(self.serialize_rpc)
            d.addCallback(lambda rpc: self.send_string(rpc.SerializeToString(),
                                                       host, port))
            service.CallMethod(method, controller, request, d.callback)

        for serializedResponse in rpc.response:
            id = serializedResponse.id
            if self._pending.has_key(id):
                self._pending[id].callback(serializedResponse)

    def send_string(self, data, host=None, port=None):
        if host and port:
            self.transport.write(data, (host, port))
        else:
            self.transport.write(data)

    def CallMethod(self, methodDescriptor, rpcController, request,
                   responseClass, done):
        rpc = self._call_method(methodDescriptor, request, responseClass, done)
        self.send_string(rpc.SerializeToString())

    def sendError(self, id, code, host, port):
        rpc = Rpc()
        rpcResponse = rpc.response.add()
        rpcResponse.id = id
        rpcResponse.error.code = code
        rpcResponse.error.text = RpcErrors.msgs[code]
        self.sendString(rpc.SerializeToString(), host, port)


class Factory(twisted.internet.protocol.Factory):
    protocol = TcpChannel

    def __init__(self, services, conn_lost_cb=None):
        """
        :param (_IPAddress, ProtocolWrapper) -> None conn_lost_cb: connection lost cb func
        :return:
        """
        self._conn_lost_cb = conn_lost_cb
        self._protocols = {}
        """:type _protocols: dict[ProtocolWrapper, _IPAddress]"""
        self._services = {}
        for s in services:
            self._services[s.GetDescriptor().name] = s

    def buildProtocol(self, addr):
        p = ProtocolWrapper(self, self.protocol())
        p._services = self._services
        return p

    def registerProtocol(self, p):
        """
        Called by protocol to register itself.
        :type p: ProtocolWrapper
        """
        self._protocols[p] = p.getPeer()

    def unregisterProtocol(self, p):
        """
        Called by protocols when they go away.
        """
        if p in self._protocols:
            if self._conn_lost_cb is not None:
                self._conn_lost_cb(self._protocols[p], p)
            del self._protocols[p]

    def closeConnectionByPeer(self, peer):
        """
        :param IPAddress peer: peer which issued this connection
        :rtype boolean: close or not
        """
        closed = False
        for protocol, _peer in self._protocols.iteritems():
            if _peer == peer:
                protocol.loseConnection()
                closed = True
        return closed



class Proxy(object):
    class _Proxy(object):
        def __init__(self, stub):
            self._stub = stub

        def __getattr__(self, key):
            def call(method, request):
                d = Deferred()
                controller = Controller()
                method(controller, request, d)
                return d

            return lambda request: call(getattr(self._stub, key), request)

    def __init__(self, *stubs):
        self._stubs = {}
        for s in stubs:
            self._stubs[s.GetDescriptor().name] = self._Proxy(s)

    def __getattr__(self, key):
        return self._stubs[key]
