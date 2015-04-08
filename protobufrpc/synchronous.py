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

import socket
import google.protobuf.service
from protobufrpc.common import Controller
from protobufrpc.protobufrpc_pb2 import Rpc, Request, Response, Error
import struct
import SocketServer
from gevent.server import StreamServer
from gevent.lock import Semaphore
import gevent

#import gevent.monkey
#if gevent.monkey.saved:
#    from gevent.server import StreamServer as SocketServer
#else:
#    import SocketServer

__all__ = ["TcpChannel", "TcpServer", "Proxy"]


class RPCException(Exception):
    pass


class RpcErrors:
    SUCCESS = 0
    UNSERIALIZE_RPC = 1
    SERVICE_NOT_FOUND = 2
    METHOD_NOT_FOUND = 3
    CANNOT_DESERIALIZE_REQUEST = 4
    METHOD_ERROR = 5

    msgs = ['Success',
            'Error when unserializing Rpc message',
            'Service not found',
            'Method not found',
            'Cannot deserialized request',
            'Method Error']


class TcpChannel(google.protobuf.service.RpcChannel):
    id = 0

    def __init__(self, addr):
        google.protobuf.service.RpcChannel.__init__(self)
        self._pending = {}
        self._tcpSocket = None
        self.connect(addr)
        self._wlock = Semaphore()
        self._rlock = Semaphore()

    def connect(self, addr):
        self._tcpSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._tcpSocket.connect(addr)

    def send_string(self, buffer):
        networkOrderBufferLen = struct.pack("!I", len(buffer))

        buffer = networkOrderBufferLen + buffer
        with self._wlock:
            sent = self._tcpSocket.sendall(buffer)
        if sent == 0:
            raise RPCException("socket connection broken")

    def CallMethod(self, methodDescriptor, rpcController, request,
                   responseClass,
                   done=None):
        self.id += 1
        self._pending[self.id] = (responseClass, done)

        rpc = Rpc()
        rpcRequest = rpc.request.add()
        rpcRequest.method = methodDescriptor.containing_service.name + '.' + methodDescriptor.name
        rpcRequest.serialized_request = request.SerializeToString()
        rpcRequest.id = self.id
        self.send_string(rpc.SerializeToString())

        with self._rlock:
            buffer = self._tcpSocket.recv(struct.calcsize("!I"))
            bufferLen = int(struct.unpack("!I", buffer)[0])
            buffer = self._tcpSocket.recv(bufferLen)

        self.string_received(buffer, rpcController)

    def string_received(self, data, rpcController):
        rpc = Rpc()
        rpc.ParseFromString(data)
        for serializedResponse in rpc.response:
            id = serializedResponse.id
            if id in self._pending and self._pending[id] is not None:
                if serializedResponse.HasField('error'):
                    rpcController.SetFailed(serializedResponse.error.text)
                    return

                responseClass = self._pending[id][0]
                done = self._pending[id][1]
                done(self.unserialize_response(serializedResponse,
                                               responseClass))

    def unserialize_response(self, serializedResponse, responseClass):
        response = responseClass()
        response.ParseFromString(serializedResponse.serialized_response)
        return response

    def serialize_response(self, response, serializedRequest):
        serializedResponse = Response()
        serializedResponse.id = serializedRequest.id
        serializedResponse.serialized_response = response.SerializeToString()
        return serializedResponse

    def serialize_rpc(self, serializedResponse):
        rpc = Rpc()
        rpcResponse = rpc.response.add()
        rpcResponse.serialized_response = serializedResponse.serialized_response
        rpcResponse.id = serializedResponse.id
        return rpc


class Proxy(object):
    class _Proxy(object):
        def __init__(self, stub):
            self._stub = stub

        def __getattr__(self, key):
            def call(method, request):
                class callbackClass(object):
                    def __init__(self):
                        self.response = None

                    def __call__(self, response):
                        self.response = response

                controller = Controller()
                callback = callbackClass()
                method(controller, request, callback)
                if controller.Failed():
                    raise RPCException(controller.ErrorText())
                return callback.response

            return lambda request: call(getattr(self._stub, key), request)

    def __init__(self, *stubs):
        self._stubs = {}
        for s in stubs:
            self._stubs[s.GetDescriptor().name] = self._Proxy(s)

    def __getattr__(self, key):
        return self._stubs[key]


class TcpServer(SocketServer.TCPServer):
    def __init__(self, host, *services):
        self.services = {}
        for s in services:
            self.services[s.GetDescriptor().name] = s
        SocketServer.TCPServer.__init__(self, host, TcpRequestHandler)


class GeventTCPServer(StreamServer):
    def __init__(self, addr, services):
        """
        >>> GeventTCPServer(('127.0.0.1', 1234), GeventTCPHandler())
        """
        handle = handle_wrapper(self)

        self.services = {}
        for s in services:
            self.services[s.GetDescriptor().name] = s

        handle.server = self

        super(GeventTCPServer, self).__init__(addr, handle)


class TcpRequestHandler(SocketServer.BaseRequestHandler):
    class callbackClass(object):
        def __init__(self):
            self.response = None

        def __call__(self, response):
            self.response = response

    def handle(self):
        while True:
            buffer = self.request.recv(struct.calcsize("!I"))
            if not buffer:
                break
            bufferLen = int(struct.unpack("!I", buffer)[0])
            buffer = self.request.recv(bufferLen)
            if not buffer:
                break
            gevent.spawn(self.string_received, buffer)

    def send_string(self, buffer):
        networkOrderBufferLen = struct.pack("!I", len(buffer))
        print "send string", [ord(i) for i in networkOrderBufferLen]

        buffer = networkOrderBufferLen + buffer
        bytesSent = 0
        while bytesSent < len(buffer):
            with self._wlock:
                sent = self.request.send(buffer[bytesSent:])
            if sent == 0:
                raise RPCException("socket connection broken")
            bytesSent += sent

    def string_received(self, data):
        rpc = Rpc()
        rpc.ParseFromString(data)
        for serializedRequest in rpc.request:
            service = self.server.services.get(serializedRequest.method.split(
                '.')[0])
            if not service:
                self.send_error(serializedRequest.id, RpcErrors.SERVICE_NOT_FOUND)
                return

            method = service.GetDescriptor().FindMethodByName(
                serializedRequest.method.split('.')[1])
            if not method:
                self.send_error(serializedRequest.id, RpcErrors.METHOD_NOT_FOUND)
                return
                
            request = service.GetRequestClass(method)()
            request.ParseFromString(serializedRequest.serialized_request)
            controller = Controller()

            callback = self.callbackClass()
            service.CallMethod(method, controller, request, callback)

            if controller.Failed():
                self.send_error(serializedRequest.id, RpcErrors.METHOD_ERROR, controller.ErrorText())
                return

            serialized_response = self.serialize_response(
                callback.response, serializedRequest)
            response_rpc = self.serialize_rpc(serialized_response)
            self.send_string(response_rpc.SerializeToString())

    def serialize_response(self, response, serialized_request):
        serializedResponse = Response()
        serializedResponse.id = serialized_request.id
        serializedResponse.serialized_response = response.SerializeToString()
        return serializedResponse

    def serialize_rpc(self, serializedResponse):
        rpc = Rpc()
        rpc.response.extend([serializedResponse])
        return rpc

    def send_error(self, _id, code, msg=None):
        rpc = Rpc()
        rpcResponse = rpc.response.add()
        rpcResponse.id = _id
        rpcResponse.error.code = code
        if not msg:
            rpcResponse.error.text = RpcErrors.msgs[code]
        else:
            rpcResponse.error.text = msg
        self.send_string(rpc.SerializeToString())


class handle_wrapper(object):
    def __init__(self, server):
        self.server = server

    def __call__(self, socket, address):
        handle = GeventTCPHandler(socket, address, self.server)
        handle.handle()


# Ensure self.request and self.server exists
class GeventTCPHandler(TcpRequestHandler):
    def __init__(self, socket, address, server):
        self.server = server
        self.request = socket
        self._wlock = Semaphore()
