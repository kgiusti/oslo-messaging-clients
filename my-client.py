#!/usr/bin/env python
#
import optparse, sys, time, logging

#from oslo.config import cfg
#from oslo import messaging

from openstack.common import context
from openstack.common.rpc import proxy
from openstack.common.rpc import CONF
from openstack.common import log

def main(argv=None):

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
    #transport = messaging.get_transport(cfg.CONF, url="qpid://localhost:5672")
    #transport = messaging.get_transport(cfg.CONF, url="messenger://0.0.0.0:5672")

    # target = messaging.Target(exchange=opts.exchange,
    #                           topic=topic,
    #                           namespace=opts.namespace,
    #                           server=opts.server,
    #                           fanout=opts.fanout,
    #                           version=opts.version)

    # client = messaging.RPCClient(transport, target,
    #                              timeout=opts.timeout,
    #                              version_cap=opts.version)

    CONF.rpc_backend = "openstack.common.rpc.impl_qpid"

    LOG = log.getLogger("openstack.common.rpc.common")
    handler = logging.StreamHandler()
    LOG.logger.addHandler(handler)
    LOG.logger.setLevel(logging.INFO)



    client = proxy.RpcProxy( topic, opts.version )

    test_context = context.RequestContext('fake_user', 'fake_project')
    # test_context = {"application": "my-client",
    #                 "time": time.ctime(),
    #                 "cast": opts.cast}

    msg = client.make_msg( method, **args )

    if opts.cast:
        client.cast( test_context, msg )
    else:
        rc = client.call( test_context, msg )
        print "Return value=%s" % str(rc)

    # @todo Need this until synchronous send available
    #transport.cleanup()

    return 0

if __name__ == "__main__":
    sys.exit(main())
