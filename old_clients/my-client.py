#!/usr/bin/env python
#
# Copyright 2014 Kenneth A. Giusti
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
#import eventlet
#eventlet.monkey_patch()
import optparse, sys, time
import logging

from oslo_config import cfg
import oslo_messaging as messaging

def handle_config_option(option, opt_string, opt_value, parser):
    name, value = opt_value
    setattr(cfg.CONF, name, value)

def main(argv=None):
    logging.warning("my-client.py has been superseded by rpc-client")
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
    parser.add_option("--oslo-config", type="string",
                      help="the oslo.messaging configuration file.")
    parser.add_option("--payload", type="string",
                      help="Path to a data file to use as message body.")
    parser.add_option("--quiet", action="store_true",
                      help="Deprecated")
    parser.add_option("--debug", action="store_true",
                      help="Enable debug logging.")
    parser.add_option("--stats", action="store_true",
                      help="Calculate throughput")

    opts, extra = parser.parse_args(args=argv)
    if not extra:
        print("Error: <topic> not supplied!!")
        return -1
    topic = extra[0]
    extra = extra[1:]
    if opts.debug:
        print("Calling server on topic %s, server=%s exchange=%s namespace=%s fanout=%s" %
              (topic, opts.server, opts.exchange, opts.namespace, str(opts.fanout)))

    if opts.debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.WARN)

    method = None
    args = {}
    if extra:
        method = extra[0]
        extra = extra[1:]
        args = dict([(extra[x], extra[x+1]) for x in range(0, len(extra)-1, 2)])
        if opts.debug: print("Method=%s, args=%s" % (method, str(args)))
    if opts.payload:
        if opts.debug: print("Loading payload file %s" % opts.payload)
        with open(opts.payload) as f:
            args["payload"] = f.read()
    if opts.oslo_config:
        if opts.debug: print("Loading config file %s" % opts.oslo_config)
        cfg.CONF(["--config-file", opts.oslo_config])

    transport = messaging.get_transport(cfg.CONF, url=opts.url)

    if opts.topology:
        if opts.debug: print("Using QPID topology version %d" % opts.topology)
        cfg.CONF.qpid_topology_version = opts.topology
    if opts.auto_delete:
        if opts.debug: print("Enable auto-delete")
        cfg.CONF.amqp_auto_delete = True
    if opts.durable:
        if opts.debug: print("Enable durable queues")
        cfg.CONF.amqp_durable_queues = True

    target = messaging.Target(exchange=opts.exchange,
                              topic=topic,
                              namespace=opts.namespace,
                              server=opts.server,
                              fanout=opts.fanout,
                              version=opts.version)

    client = messaging.RPCClient(transport, target,
                                 timeout=opts.timeout,
                                 version_cap=opts.version).prepare()

    test_context = {"application": "my-client",
                    "time": time.ctime(),
                    "cast": opts.cast}

    start_time = time.time()
    repeat = 0
    while opts.repeat == 0 or repeat < opts.repeat:
        try:
            if opts.cast or opts.fanout:
                client.cast( test_context, method, **args )
            else:
                rc = client.call( test_context, method, **args )
                if opts.debug: print("RPC return value=%s" % str(rc))
        except KeyboardInterrupt:
            break
        except Exception as e:
            logging.error("Unexpected exception occured: %s" % str(e))
            raise
        repeat += 1

    if opts.stats:
        delta = time.time() - start_time
        stats = float(repeat)/float(delta) if delta else 0
        print("Messages per second: %6.4f" % stats)

    # @todo Need this until synchronous send available
    logging.info("RPC complete!  Cleaning up transport...")
    time.sleep(0)
    transport.cleanup()

    return 0

if __name__ == "__main__":
    sys.exit(main())
