#!/usr/bin/env python
#
from oslo.config import cfg
from oslo import messaging

class ServerControlEndpoint(object):
    def __init__(self, server):
        self.server = server

    def stop(self, ctx):
        print "ServerControlEndpoint::stop() called!!!"
        # self.server.stop()

class TestEndpoint(object):
    def test(self, ctx, arg):
        print "TestEndpoint::test() called!!!"
        return arg



transport = messaging.get_transport(cfg.CONF, url="qpid://localhost:5672")

target = messaging.Target(exchange="my-exchange",
                          topic='my-topic',
                          namespace='my-namespace',
                          server='my-server-name')

endpoints = [
    ServerControlEndpoint(None),
    TestEndpoint(),
]
server = messaging.get_rpc_server(transport, target, endpoints)
server.start()
server.wait()
