#!/usr/bin/env python
#
import optparse, sys

from oslo.config import cfg
from oslo import messaging

class TestEndpoint01(object):
    def __init__(self, target=None):
        self.target = target

    def methodA(self, ctx, **args):
        print "TestEndpoint01::methodA( ctxt=%s arg=%s ) called!!!" % (str(ctx),str(args))
    def common(self, ctx, **args):
        print "TestEndpoint01::common( ctxt=%s arg=%s ) called!!!" % (str(ctx),str(args))

class TestEndpoint02(object):
    def __init__(self, target=None):
        self.target = target

    def methodB(self, ctx, **args):
        print "TestEndpoint02::methodB( ctxt=%s arg=%s ) called!!!" % (str(ctx),str(args))
        return ctx
    def common(self, ctx, **args):
        print "TestEndpoint02::common( ctxt=%s arg=%s ) called!!!" % (str(ctx),str(args))


def main(argv=None):


    _usage = """Usage: %prog [options]"""
    parser = optparse.OptionParser(usage=_usage)
    parser.add_option("--exchange", action="store", default="my-exchange")
    parser.add_option("--topic", action="store", default="my-topic")
    parser.add_option("--server", action="store", default="my-server-name")
    parser.add_option("--namespace", action="store", default="my-namespace")
    parser.add_option("--version", action="store", default="1.1")

    opts, extra = parser.parse_args(args=argv)
    print "Running server, name=%s exchange=%s topic=%s namespace=%s" % (
        opts.server, opts.exchange, opts.topic, opts.namespace)

    transport = messaging.get_transport(cfg.CONF, url="qpid://localhost:5672")

    target = messaging.Target(exchange=opts.exchange,
                              topic=opts.topic,
                              namespace=opts.namespace,
                              server=opts.server,
                              version=opts.version)

    endpoints = [
        TestEndpoint01(target),
        TestEndpoint02(target),
        ]
    server = messaging.get_rpc_server(transport, target, endpoints)
    server.start()
    server.wait()
    return 0

if __name__ == "__main__":
    sys.exit(main())
