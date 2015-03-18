#!/usr/bin/env python
#
# Copyright 2015 Kenneth A. Giusti
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

import optparse
import sys
import uuid

from oslo_config import cfg
import oslo_messaging

import logging
#loggy = logging.getLogger("oslo_messaging.notify.dispatcher")
loggy = logging.getLogger("oslo_messaging._drivers.protocols.amqp.driver")
loggy.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
loggy.addHandler(ch)


class TestNotificationEndpoint(object):
    def __init__(self, name):
        self.name = name

    def debug(self, ctx, publisher, event_type, payload, metadata):
        print("%s:debug:%s:%s:%s:%s"
              % (self.name, publisher, event_type, payload, metadata))


def main(argv=None):
    _usage = """Usage: %prog [options] <target, ...>"""
    parser = optparse.OptionParser(usage=_usage)
    parser.add_option("--name", action="store", default=uuid.uuid4().hex)
    parser.add_option("--url", action="store", default="qpid://localhost")
    parser.add_option("--exchange", action="store")
    parser.add_option("--namespace", action="store")
    parser.add_option("--pool", action="store")
    parser.add_option("--quiet", action="store_true", default=False,
                      help="Supress console output")

    opts, topics = parser.parse_args(args=argv)
    if not topics:
        if not opts.quiet: print "missing topics!"
        return -1

    if not opts.quiet:
        print("listener %s: url=%s, topics=%s" % (opts.name, opts.url, topics))
    targets = [oslo_messaging.Target(exchange=opts.exchange,
                                     topic=t,
                                     namespace=opts.namespace) for t in topics]

    transport = oslo_messaging.get_transport(cfg.CONF, url=opts.url)
    listener = oslo_messaging.get_notification_listener(transport,
                                                        targets,
                                                        [TestNotificationEndpoint(opts.name)],
                                                        pool=opts.pool)
    try:
        listener.start()
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopping..")
    listener.stop()
    listener.wait()
    return 0

if __name__ == "__main__":
    sys.exit(main())

