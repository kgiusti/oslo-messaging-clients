#!/usr/bin/env python
#
#import eventlet
#eventlet.monkey_patch()
import optparse, sys, time
import logging

from oslo.config import cfg
from oslo import messaging

loggy = logging.getLogger("oslo.messaging._drivers.impl_messenger")
loggy.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
loggy.addHandler(ch)


def main(argv=None):

    print "I'M not done yet!"
    return 0

    _usage = """Usage: %prog [options] <topic> <method> [<arg-name> <arg-value>]*"""
    parser = optparse.OptionParser(usage=_usage)
    parser.add_option("--exchange", action="store", default="my-exchange")
    #parser.add_option("--topic", action="store", default="my-topic")
    parser.add_option("--server", action="store")
    parser.add_option("--namespace", action="store", default="my-namespace")
    parser.add_option("--fanout", action="store_true")
    parser.add_option("--timeout", action="store", type="int")
    parser.add_option("--cast", action="store_true")
    parser.add_option("--version", action="store", default="1.1")
    parser.add_option("--messenger", action="store_true",
                      help="Use experimental Messenger transport")
    parser.add_option("--topology", action="store", type="int",
                      help="QPID Topology version to use.")

    opts, extra = parser.parse_args(args=argv)
    if not extra:
        print "<topic> not supplied!!"
        return -1
    topic = extra[0]
    extra = extra[1:]
    print "Calling server on topic %s, server=%s exchange=%s namespace=%s fanout=%s" % (
        topic, opts.server, opts.exchange, opts.namespace, str(opts.fanout))

    method = None
    args = None
    if extra:
        method = extra[0]
        extra = extra[1:]
        args = dict([(extra[x], extra[x+1]) for x in range(0, len(extra)-1, 2)])
        print "Method=%s, args=%s" % (method, str(args))

    # @todo Fails with Dispatch?
    if opts.messenger:
        print "Using Messenger transport!"
        _url = "messenger://0.0.0.0:5672"
    else:
        _url = "qpid://localhost:5672"

    transport = messaging.get_transport(cfg.CONF, url=_url)

    if opts.topology:
        print "Using QPID topology version %d" % opts.topology
        cfg.CONF.qpid_topology_version = opts.topology

    notifier = messaging.Notifier(transport, topic)

    test_context = {"application": "notifier",
                    "time": time.ctime(),
                    "cast": opts.cast}

    print "NOTIFYING..."
    notifier.info( test_context, "my-event-type", {"arg1": 1, "arg2": 2,
                                                   "arg3": "foobar"} );

    # @todo Need this until synchronous send available
    transport.cleanup()

    return 0

if __name__ == "__main__":
    sys.exit(main())
