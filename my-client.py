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

def handle_config_option(option, opt_string, opt_value, parser):
    name, value = opt_value
    print "Name=%s" % name
    print "Value=%s" % value
    setattr(cfg.CONF, name, value)

def main(argv=None):

    _usage = """Usage: %prog [options] <topic> <method> [<arg-name> <arg-value>]*"""
    parser = optparse.OptionParser(usage=_usage)
    parser.add_option("--exchange", action="store", default="my-exchange")
    parser.add_option("--server", action="store")
    parser.add_option("--namespace", action="store", default="my-namespace")
    parser.add_option("--fanout", action="store_true")
    parser.add_option("--timeout", action="store", type="int")
    parser.add_option("--cast", action="store_true")
    parser.add_option("--repeat", action="store", type="int", default=1,
                      help="Repeat the request N times (0=forever)")
    parser.add_option("--version", action="store", default="1.1")
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
        print "<topic> not supplied!!"
        return -1
    topic = extra[0]
    extra = extra[1:]
    print "Calling server on topic %s, server=%s exchange=%s namespace=%s fanout=%s" % (
        topic, opts.server, opts.exchange, opts.namespace, str(opts.fanout))


    logging.basicConfig(level=logging.INFO)  #make this an option
    method = None
    args = None
    if extra:
        method = extra[0]
        extra = extra[1:]
        args = dict([(extra[x], extra[x+1]) for x in range(0, len(extra)-1, 2)])
        print "Method=%s, args=%s" % (method, str(args))

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
                              topic=topic,
                              namespace=opts.namespace,
                              server=opts.server,
                              fanout=opts.fanout,
                              version=opts.version)

    client = messaging.RPCClient(transport, target,
                                 timeout=opts.timeout,
                                 version_cap=opts.version)

    test_context = {"application": "my-client",
                    "time": time.ctime(),
                    "cast": opts.cast}

    repeat = 0
    while opts.repeat == 0 or repeat < opts.repeat:
        try:
            if opts.cast:
                client.cast( test_context, method, **args )
            else:
                rc = client.call( test_context, method, **args )
                print "Return value=%s" % str(rc)
        except Exception as e:
            loggy.error("Unexpected exception occured: %s" % str(e))
            return -1
        repeat += 1

    # @todo Need this until synchronous send available
    transport.cleanup()

    return 0

if __name__ == "__main__":
    sys.exit(main())
