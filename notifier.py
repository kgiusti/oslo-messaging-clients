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
#import eventlet
#eventlet.monkey_patch()
import optparse, sys, time
import uuid

from oslo_config import cfg
import oslo_messaging

import logging


def main(argv=None):
    _usage = """Usage: %prog [options] <topic>"""
    parser = optparse.OptionParser(usage=_usage)
    parser.add_option("--name", action="store", default=uuid.uuid4().hex)
    parser.add_option("--url", action="store", default="qpid://localhost")
    parser.add_option("--quiet", action="store_true", default=False,
                      help="Supress console output")
    parser.add_option("--type", action="store", default="event")
    parser.add_option("--payload", action="store", default="payload")
    parser.add_option("--count", action="store", type="int", default=1)
    parser.add_option("--debug", action="store_true",
                      help="Enable debug logging.")

    opts, topic = parser.parse_args(args=argv)
    if not topic:
        if not opts.quiet: print("missing topic!")
        return -1
    topic = topic[0]

    if opts.debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.WARN)

    if not opts.quiet:
        print("notifier %s: url=%s, topic=%s" % (opts.name, opts.url, topic))

    transport = oslo_messaging.get_transport(cfg.CONF, url=opts.url)
    n = oslo_messaging.notify.notifier.Notifier(transport, opts.name,
                                                driver='messaging',
                                                topic=topic)
    for i in range(opts.count):
        n.debug({}, opts.type, {"payload":opts.payload})
    return 0

if __name__ == "__main__":
    sys.exit(main())
