#!/usr/bin/env python
# -*- coding: utf-8 -*-
import collections
import glob
import gzip
import logging
import os
import sys
import threading
from optparse import OptionParser
# brew install protobuf
# protoc  --python_out=. ./appsinstalled.proto
# pip install protobuf
import appsinstalled_pb2
# pip install python-memcached
import memcache

NORMAL_ERR_RATE = 0.01
AppsInstalled = collections.namedtuple(
    "AppsInstalled",
    ["dev_type", "dev_id", "lat", "lon", "apps"]
)

STATUS_SKIP = -1
STATUS_ERR = 0
STATUS_OK = 1

McConnection = collections.namedtuple(
    "McConnection",
    ["client", "lock"]
)


def init_mc_connections(options):
    def connect(addr):
        # @TODO retry and timeouts!
        return memcache.Client([addr])

    return {
        # TODO loop
        "idfa": McConnection(connect(options.idfa), threading.Lock()),
        "gaid": McConnection(connect(options.gaid), threading.Lock()),
        "adid": McConnection(connect(options.adid), threading.Lock()),
        "dvid": McConnection(connect(options.dvid), threading.Lock()),
    }


def close_mc_connections(mc_connections):
    pass


def put_to_mc(mc_connections, dev_type, key, val, dry_run=False):
    mc_connection = mc_connections.get(dev_type)
    if not mc_connection:
        logging.error("Unknow device type: %s", dev_type)
        return STATUS_ERR
    if dry_run:
        logging.debug("%s - %s -> %s" % (mc_connection.client, key, str(val).replace("\n", " ")))
        return STATUS_OK

    try:
        mc_connection.client.set(key, val)
        return STATUS_OK
    except Exception, e:
        logging.exception("Cannot write to memc %s: %s", mc_connection.client, e)
        return STATUS_ERR


def dot_rename(path):
    head, fn = os.path.split(path)
    # atomic in most cases
    os.rename(path, os.path.join(head, "." + fn))


def make_protobuf_struct(appsinstalled):
    ua = appsinstalled_pb2.UserApps()
    ua.lat = appsinstalled.lat
    ua.lon = appsinstalled.lon
    ua.apps.extend(appsinstalled.apps)
    return ua


def make_key(appsinstalled):
    return "%s:%s" % (appsinstalled.dev_type, appsinstalled.dev_id)


def parse_appsinstalled(line):
    line_parts = line.strip().split("\t")
    if len(line_parts) < 5:
        return
    dev_type, dev_id, lat, lon, raw_apps = line_parts
    if not dev_type or not dev_id:
        return

    try:
        apps = [int(a.strip()) for a in raw_apps.split(",")]
    except ValueError:
        apps = [int(a.strip()) for a in raw_apps.split(",") if a.isidigit()]
        logging.info("Not all user apps are digits: `%s`" % line)

    try:
        lat, lon = float(lat), float(lon)
    except ValueError:
        logging.info("Invalid geo coords: `%s`" % line)
        return

    return AppsInstalled(dev_type, dev_id, lat, lon, apps)


def process_one_line(line, mc_connections, dry_run):
    line = line.strip()
    if not line:
        return STATUS_SKIP
    appsinstalled = parse_appsinstalled(line)
    if not appsinstalled:
        return STATUS_ERR
    protobuf_struct = make_protobuf_struct(appsinstalled)
    return put_to_mc(mc_connections,
                     dev_type=appsinstalled.dev_type,
                     key=make_key(appsinstalled),
                     value=protobuf_struct.SerializeToString(),
                     dry_run=dry_run)


def process_one_file(fn, mc_connections, dry_run):
    processed = errors = 0
    logging.info('Processing %s', fn)
    fd = gzip.open(fn)
    for line in fd:
        status = process_one_line(line, mc_connections, dry_run)
        if status == STATUS_OK:
            processed += 1
        else:
            errors += 1

    if processed:
        err_rate = float(errors) / processed
        if err_rate < NORMAL_ERR_RATE:
            logging.info("Acceptable error rate (%s). Successfull load" % err_rate)
        else:
            logging.error("High error rate (%s > %s). Failed load" % (err_rate, NORMAL_ERR_RATE))

    fd.close()
    dot_rename(fn)


def main(options):
    mc_connections = init_mc_connections(options)
    for fn in glob.iglob(options.pattern):
        process_one_file(fn, mc_connections)


def prototest():
    sample = (
        "idfa\t1rfw452y52g2gq4g\t55.55\t42.42\t1423,43,567,3,7,23\n"
        "gaid\t7rfw452y52g2gq4g\t55.55\t42.42\t7423,424"
    )
    for line in sample.splitlines():
        dev_type, dev_id, lat, lon, raw_apps = line.strip().split("\t")
        apps = [int(a) for a in raw_apps.split(",") if a.isdigit()]
        lat, lon = float(lat), float(lon)
        ua = appsinstalled_pb2.UserApps()
        ua.lat = lat
        ua.lon = lon
        ua.apps.extend(apps)
        packed = ua.SerializeToString()
        unpacked = appsinstalled_pb2.UserApps()
        unpacked.ParseFromString(packed)
        assert ua == unpacked


if __name__ == '__main__':
    op = OptionParser()
    op.add_option("-t", "--test", action="store_true", default=False)
    op.add_option("-l", "--log", action="store", default=None)
    op.add_option("--dry", action="store_true", default=False)
    op.add_option("--pattern", action="store", default="/data/appsinstalled/*.tsv.gz")
    op.add_option("--idfa", action="store", default="127.0.0.1:33013")
    op.add_option("--gaid", action="store", default="127.0.0.1:33014")
    op.add_option("--adid", action="store", default="127.0.0.1:33015")
    op.add_option("--dvid", action="store", default="127.0.0.1:33016")
    (opts, args) = op.parse_args()
    logging.basicConfig(filename=opts.log, level=logging.INFO if not opts.dry else logging.DEBUG,
                        format='[%(asctime)s] %(levelname).1s %(message)s', datefmt='%Y.%m.%d %H:%M:%S')
    if opts.test:
        prototest()
        sys.exit(0)

    logging.info("Memc loader started with options: %s" % opts)
    try:
        main(opts)
    except Exception, e:
        logging.exception("Unexpected error: %s" % e)
        sys.exit(1)
