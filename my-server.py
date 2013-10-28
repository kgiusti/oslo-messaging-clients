#!/usr/bin/env python
#
import optparse, sys, time, logging

#from oslo.config import cfg
#from oslo import messaging
from openstack.common.rpc import service
from openstack.common.rpc import CONF
from openstack.common import log
from openstack.common import service as _service


class TestEndpoint01(object):
    def __init__(self, server, target=None, version=None):
        self.server = server
        self.target = target
        self.RPC_API_VERSION = version

    def methodA(self, ctx, **args):
        print("%s::TestEndpoint01::methodA( ctxt=%s arg=%s ) called!!!"
              % (self.server, str(ctx),str(args)))
    def methodB(self, ctx, **args):
        print("%s::TestEndpoint01::methodB( ctxt=%s arg=%s ) called!!!"
              % (self.server, str(ctx),str(args)))
        #return ctx
        return {"result":"OK"}
    def common(self, ctx, **args):
        print("%s::TestEndpoint01::common( ctxt=%s arg=%s ) called!!!"
              % (self.server, str(ctx),str(args)))

class TestEndpoint02(object):
    def __init__(self, server, target=None, version=None):
        self.server = server
        self.target = target
        self.RPC_API_VERSION = version

    def methodB(self, ctx, **args):
        print("%s::TestEndpoint02::methodB( ctxt=%s arg=%s ) called!!!"
              % (self.server, str(ctx),str(args)))
        #return ctx
        return {"result":"Meh"}
    def common(self, ctx, **args):
        print("%s::TestEndpoint02::common( ctxt=%s arg=%s ) called!!!"
              % (self.server, str(ctx),str(args)))


def main(argv=None):

    _usage = """Usage: %prog [options] <name>"""
    parser = optparse.OptionParser(usage=_usage)
    parser.add_option("--exchange", action="store", default="my-exchange")
    parser.add_option("--topic", action="store", default="my-topic")
    #parser.add_option("--server", action="store", default="my-server-name")
    parser.add_option("--namespace", action="store", default="my-namespace")
    parser.add_option("--version", action="store", default="1.1")
    parser.add_option("--eventlet", action="store_true")

    opts, extra = parser.parse_args(args=argv)
    if not extra:
        print "<name> not supplied!"
        return -1
    server_name = extra[0]

    ## No handlers could be found for logger "openstack.common.rpc.common"
    # LOG = log.getLogger("openstack.common.rpc.common")
    # handler = logging.StreamHandler()
    # LOG.logger.addHandler(handler)
    # LOG.logger.setLevel(logging.INFO)

    print "Running server, name=%s exchange=%s topic=%s namespace=%s" % (
        server_name, opts.exchange, opts.topic, opts.namespace)

    # @todo Dispatch fails with localhost?
    #transport = messaging.get_transport(cfg.CONF, url="qpid://localhost:5672")
    #transport = messaging.get_transport(cfg.CONF, url="messenger://0.0.0.0:5672")
    CONF.rpc_backend = "openstack.common.rpc.impl_qpid"

    #transport = messaging.get_transport(cfg.CONF, url="qpid://localhost:5672")

    # target = messaging.Target(exchange=opts.exchange,
    #                           topic=opts.topic,
    #                           namespace=opts.namespace,
    #                           server=server_name,
    #                           version=opts.version)

    endpoints = [
        TestEndpoint01(server_name, opts.topic, opts.version),
        TestEndpoint02(server_name, opts.topic, opts.version),
        ]
    # server = messaging.get_rpc_server(transport, target, endpoints,
    #                                   executor='eventlet' if opts.eventlet else 'blocking')

    server = service.Service( server_name, opts.topic,
                              TestEndpoint01(server_name, opts.topic, opts.version))

    l = _service.launch(server)

    try:
        #server.start()
        l.wait()
        while True:
            time.sleep(1)
            # sys.stdout.write('.')
            # sys.stdout.flush()
    except KeyboardInterrupt:
        print("Stopping..")
        #server.stop()
        #server.wait()
    return 0

if __name__ == "__main__":
    sys.exit(main())
