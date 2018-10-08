#!/usr/bin/env python
#
# Copyright 2016 Kenneth A. Giusti
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

import sys
import threading
import time

from oslo_log import log as logging
from oslo_config import cfg
import oslo_messaging as messaging

LOG = logging.getLogger(__name__)

_options = [
    cfg.StrOpt("method",
               help="The name of the method to call on the server"),
    cfg.StrOpt("kwargs",
               help="Method arguments in the form 'name=value ...'"),
    cfg.StrOpt("topic",
               default="my-topic",
               help="target topic, default 'my-topic'"),
    cfg.StrOpt('exchange',
               default="my-exchange",
               help="target exchange, default 'my-exchange'"),
    cfg.StrOpt('namespace',
               help="target namespace, default None"),
    cfg.StrOpt("url",
               required=True,
               default="rabbit://localhost:5672",
               help="transport address, default 'rabbit://localhost:5672'"),
    cfg.StrOpt("target_version",
               help="Override the default version in the target address"),
    cfg.BoolOpt("quiet",
                default=False,
                help="Suppress all stdout output"),
    cfg.StrOpt("server",
               help="Send only to the named server"),
    cfg.IntOpt("timeout",
               default=60,
               help="timeout RPC request in seconds, default 60"),
    cfg.BoolOpt("fanout",
                default=False,
                help="Send RPC fanout cast"),
    cfg.BoolOpt("cast",
                default=False,
                help="Send RPC cast"),
    cfg.IntOpt("repeat",
               default=1,
               help="Repeat the request N times per thread (0=forever)"),
    cfg.StrOpt("payload",
               help="Path to a data file to use as message body."),
    cfg.BoolOpt("stats",
                default=False,
                help="Calculate throughput"),
    cfg.IntOpt("threads",
               default=1,
               help="Number of threads (default 1)"),
    cfg.StrOpt("log_levels",
               help="Set module specific log levels, e.g."
               " 'amqp=WARN,oslo.messaging=INFO,...'")
]


class ClientThread(threading.Thread):
    def __init__(self, conf, transport, target, method, kwargs):
        super(ClientThread, self).__init__()
        self.transport = transport
        self.target = target
        self.conf = conf
        self.method = method
        self.kwargs = kwargs
        self.daemon = True
        self.start()

    def run(self):
        conf = self.conf
        client = messaging.RPCClient(self.transport,
                                     self.target,
                                     timeout=conf.timeout,
                                     version_cap=conf.target_version).prepare()
        test_context = {"application": "rpc-client", "time": time.ctime()}

        repeat = 0
        while conf.repeat == 0 or repeat < conf.repeat:
            try:
                if conf.cast or conf.fanout:
                    client.cast(test_context, self.method, **self.kwargs)
                else:
                    rc = client.call(test_context, self.method, **self.kwargs)
                    if not conf.quiet: print("RPC return value=%s" % str(rc))
            except Exception as e:
                LOG.error("Unexpected exception occured: %s" % str(e))
                break
            repeat += 1


def main(argv=None):

    logging.register_options(cfg.CONF)
    cfg.CONF.register_cli_opts(_options)
    cfg.CONF(sys.argv[1:])
    if cfg.CONF.log_levels:
        logging.set_defaults(
            default_log_levels=cfg.CONF.log_levels.split(',')
        )
    logging.setup(cfg.CONF, "rpc-client")

    quiet = cfg.CONF.quiet
    server = cfg.CONF.server
    exchange = cfg.CONF.exchange
    topic = cfg.CONF.topic
    namespace = cfg.CONF.namespace
    url = cfg.CONF.url
    target_version = cfg.CONF.target_version
    fanout = cfg.CONF.fanout
    cast = cfg.CONF.cast
    payload = cfg.CONF.payload

    method = cfg.CONF.method or "echo"
    args = cfg.CONF.kwargs.split() if cfg.CONF.kwargs else []
    kwargs = dict([(x.split('=')[0], x.split('=')[1] if len(x) > 1 else None)
                   for x in args])
    if not quiet:
        print("Calling %s (%s) on server=%s exchange=%s topic=%s namespace=%s"
              " fanout=%s cast=%s"
              % (method, kwargs, server, exchange, topic,
                 namespace, str(fanout), str(cast)))

    if payload:
        if not quiet:
            print("Loading payload file %s" % payload)
        with open(payload) as f:
            kwargs["payload"] = f.read()

    transport = messaging.get_rpc_transport(cfg.CONF, url=url)

    target = messaging.Target(exchange=exchange,
                              topic=topic,
                              namespace=namespace,
                              server=server,
                              fanout=fanout,
                              version=target_version)
    threads = []
    start_time = time.time()
    for t in range(cfg.CONF.threads):
        threads.append(ClientThread(cfg.CONF, transport, target,
                                    method, kwargs))
    try:
        for t in threads:
            t.join()
    except KeyboardInterrupt:
        pass

    if cfg.CONF.stats:
        delta = time.time() - start_time
        msg_count = cfg.CONF.repeat * cfg.CONF.threads
        stats = float(msg_count)/float(delta) if delta else 0
        print("Messages per second: %6.4f" % stats)

    # @todo Need this until synchronous send available
    if not quiet:
        print("RPC complete!  Cleaning up transport...")
    time.sleep(0)
    transport.cleanup()
    return 0

if __name__ == "__main__":
    sys.exit(main())
