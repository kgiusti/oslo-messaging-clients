#!/usr/bin/env python
#
#import eventlet
#eventlet.monkey_patch()
import optparse, sys, time
import logging

from oslo.config import cfg
from oslo import messaging

loggy = logging.getLogger(__name__)
loggy.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
loggy.addHandler(ch)


class TestEndpoint01(object):
    def __init__(self, server, target=None):
        self.server = server
        self.target = target

    def sink(self, ctx, **args):
        """Drop the message - no reply sent."""
        print("%s::TestEndpoint01:sink( ctxt=%s arg=%s ) called!!!"
              % (self.server, str(ctx),str(args)))

    def echo(self, ctx, **args):
        print("%s::TestEndpoint01::echo( ctxt=%s arg=%s ) called!!!"
              % (self.server, str(ctx),str(args)))
        return ctx

    def methodA(self, ctx, **args):
        print("%s::TestEndpoint01::methodA( ctxt=%s arg=%s ) called!!!"
              % (self.server, str(ctx),str(args)))
    def common(self, ctx, **args):
        print("%s::TestEndpoint01::common( ctxt=%s arg=%s ) called!!!"
              % (self.server, str(ctx),str(args)))

    def sleep5(self, ctx, **args):
        print("%s::TestEndpoint01::sleep5( ctxt=%s arg=%s ) called!!!"
              % (self.server, str(ctx),str(args)))
        print("   sleeping...");
        time.sleep(5)
        print("   ...awake!");

class TestEndpoint02(object):
    def __init__(self, server, target=None):
        self.server = server
        self.target = target

    def methodB(self, ctx, **args):
        print("%s::TestEndpoint02::methodB( ctxt=%s arg=%s ) called!!!"
              % (self.server, str(ctx),str(args)))
        return ctx
    def common(self, ctx, **args):
        print("%s::TestEndpoint02::common( ctxt=%s arg=%s ) called!!!"
              % (self.server, str(ctx),str(args)))

def handle_config_option(option, opt_string, opt_value, parser):
    name, value = opt_value
    setattr(cfg.CONF, name, value)

def main(argv=None):

    _usage = """Usage: %prog [options] <name>"""
    parser = optparse.OptionParser(usage=_usage)
    parser.add_option("--exchange", action="store", default="my-exchange")
    parser.add_option("--topic", action="store", default="my-topic")
    #parser.add_option("--server", action="store", default="my-server-name")
    parser.add_option("--namespace", action="store", default="my-namespace")
    parser.add_option("--version", action="store", default="1.1")
    parser.add_option("--eventlet", action="store_true")
    parser.add_option("--url", action="store", default="qpid://localhost")
    parser.add_option("--topology", action="store", type="int", default=2,
                      help="QPID Topology version to use.")
    parser.add_option("--auto-delete", action="store_true",
                      help="Set amqp_auto_delete to True")
    parser.add_option("--durable", action="store_true",
                      help="Set amqp_durable_queues to True")
    parser.add_option("--config", action="callback",
                      callback=handle_config_option, nargs=2, type="string",
                      help="set a config variable (--config name value)")

    opts, extra = parser.parse_args(args=argv)
    if not extra:
        print "<name> not supplied!"
        return -1
    server_name = extra[0]

    print "Running server, name=%s exchange=%s topic=%s namespace=%s" % (
        server_name, opts.exchange, opts.topic, opts.namespace)
    logging.basicConfig(level=logging.INFO)  #make this an option

    transport = messaging.get_transport(cfg.CONF, url=opts.url)

    if opts.topology:
        print "Using QPID topology version %d" % opts.topology
        cfg.CONF.qpid_topology_version = opts.topology
    if opts.auto_delete:
        print "Enable auto-delete"
        cfg.CONF.amqp_auto_delete = True
    if opts.durable:
        print "Enable durable queues"
        cfg.CONF.amqp_durable_queues = True


    target = messaging.Target(exchange=opts.exchange,
                              topic=opts.topic,
                              namespace=opts.namespace,
                              server=server_name,
                              version=opts.version)

    endpoints = [
        TestEndpoint01(server_name, target),
        TestEndpoint02(server_name, target),
        ]
    server = messaging.get_rpc_server(transport, target, endpoints,
                                      executor='eventlet' if opts.eventlet else 'blocking')

    try:
        server.start()
        while True:
            time.sleep(1)
            sys.stdout.write('.')
            sys.stdout.flush()
    except KeyboardInterrupt:
        print("Stopping..")
        server.stop()
        server.wait()
    return 0

if __name__ == "__main__":
    sys.exit(main())
