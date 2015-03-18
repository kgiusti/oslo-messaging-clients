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
loggy = logging.getLogger("oslo_messaging.notify.dispatcher")
loggy.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
loggy.addHandler(ch)


def main(argv=None):
    _usage = """Usage: %prog [options] <topic>"""
    parser = optparse.OptionParser(usage=_usage)
    parser.add_option("--name", action="store", default=uuid.uuid4().hex)
    parser.add_option("--url", action="store", default="qpid://localhost")
    parser.add_option("--quiet", action="store_true", default=False,
                      help="Supress console output")
    parser.add_option("--type", action="store", default="event")
    parser.add_option("--payload", action="store", default="payload")

    opts, topic = parser.parse_args(args=argv)
    if not topic:
        if not opts.quiet: print "missing topic!"
        return -1
    topic = topic[0]

    if not opts.quiet:
        print("notifier %s: url=%s, topic=%s" % (opts.name, opts.url, topic))

    transport = oslo_messaging.get_transport(cfg.CONF, url=opts.url)
    n = oslo_messaging.notify.notifier.Notifier(transport, opts.name,
                                                driver='messaging',
                                                topic=topic)
    return n.debug({}, opts.type, {"payload":opts.payload})

if __name__ == "__main__":
    sys.exit(main())
