#!/usr/bin/env python
# -*- coding: utf-8 -*-
import collections
import functools
import glob
import gzip
import logging
import os
import sys
import threading
import Queue
from optparse import OptionParser
# brew install protobuf
# protoc  --python_out=. ./appsinstalled.proto
# pip install protobuf
import appsinstalled_pb2
# pip install python-memcached
import memcache

NUM_THREADS = 4
BATCH_SIZE = 10
NORMAL_ERR_RATE = 0.01
AppsInstalled = collections.namedtuple(
    "AppsInstalled",
    ["dev_type", "dev_id", "lat", "lon", "apps"]
)

STATUS_ERR = 0
STATUS_OK = 1

McConnection = collections.namedtuple(
    "McConnection",
    ["addr", "client", "lock"]
)


def retry_if_fails(num_retries):
    if num_retries < 0:
        raise ValueError('num_retries must be >= 0')

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            num_retries_ = num_retries
            while num_retries_:
                try:
                    return func(*args, **kwargs)
                except Exception:
                    num_retries_ -= 1
            return func(*args, **kwargs)

        return wrapper

    return decorator


def init_mc_connections(options):
    def connect(addr):
        return memcache.Client([addr])

    mc_connections = {}
    for dev_type in ['idfa', 'gaid', 'adid', 'dvid']:
        addr = getattr(options, dev_type)
        mc_connections[dev_type] = McConnection(addr, connect(addr), threading.Lock())

    return mc_connections


class Worker(threading.Thread):

    SENTINEL = 'quit'

    def __init__(self, queue):
        super(Worker, self).__init__()
        self.queue = queue

    def run(self):
        while True:
            tasks_batch = self.queue.get()
            self.queue.task_done()
            if isinstance(tasks_batch, str) and tasks_batch == self.SENTINEL:
                break
            for task in tasks_batch:
                task.execute()


class UploadTask(object):
    def __init__(self, mc_connection, key, protobuf_struct, results_list, dry_run=False):
        self.mc_connection = mc_connection
        self.key = key
        self.protobuf_struct = protobuf_struct
        self.results_list = results_list
        self.dry_run = dry_run

    def execute(self):
        logging.debug('run in thread %s: %s', threading.currentThread().ident, self)
        if self.dry_run:
            self.results_list.append(STATUS_OK)
            return

        try:
            with self.mc_connection.lock:
                self._set_mc_value()
            self.results_list.append(STATUS_OK)
        except Exception, e:
            logging.exception("Cannot write to memc %s: %s", self.mc_connection.addr, e)
            self.results_list.append(STATUS_ERR)

    @retry_if_fails(5)
    def _set_mc_value(self):
        self.mc_connection.client.set(self.key, self.protobuf_struct.SerializeToString())

    def __repr__(self):
        return "%s - %s -> %s" % (self.mc_connection.addr,
                                  self.key,
                                  str(self.protobuf_struct).replace("\n", " "))


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


def make_upload_task(line, mc_connections, results_list, dry_run):
    line = line.strip()
    if not line:
        return
    appsinstalled = parse_appsinstalled(line)
    if not appsinstalled:
        raise ValueError('failed to parse')
    protobuf_struct = make_protobuf_struct(appsinstalled)

    mc_connection = mc_connections.get(appsinstalled.dev_type)
    if not mc_connection:
        raise RuntimeError("Unknow device type: %s", appsinstalled.dev_type)

    key = make_key(appsinstalled)
    return UploadTask(mc_connection,
                      key=key,
                      protobuf_struct=protobuf_struct,
                      results_list=results_list,
                      dry_run=dry_run)


def log_err_stat(fn, results_list):
    processed = results_list.count(STATUS_OK)
    errors = results_list.count(STATUS_ERR)
    err_rate = float(errors) / processed
    if err_rate < NORMAL_ERR_RATE:
        logging.info("Acceptable error rate (%s). Successfull load %s", err_rate, fn)
    else:
        logging.error("High error rate (%s > %s). Failed load %s", err_rate, NORMAL_ERR_RATE, fn)


def process_one_file(fn, mc_connections, tasks_queue, dry_run):
    results_list = []
    logging.info('Processing %s', fn)
    fd = gzip.open(fn)
    tasks_batch = []
    for line in fd:
        try:
            tasks_batch.append(make_upload_task(line, mc_connections, results_list, dry_run))
        except Exception as e:
            logging.error(e)
            results_list.append(STATUS_ERR)
        if len(tasks_batch) == BATCH_SIZE:
            tasks_queue.put(tasks_batch)
            tasks_batch = []

    # add tasks from last iterations
    tasks_queue.put(tasks_batch)
    tasks_queue.join()
    log_err_stat(fn, results_list)

    fd.close()
    dot_rename(fn)


def make_workers(queue, size):
    workers = []
    for _ in range(size):
        worker = Worker(queue)
        worker.start()
        workers.append(worker)
    return workers


def main(options):
    mc_connections = init_mc_connections(options)
    tasks_queue = Queue.Queue()
    workers = make_workers(tasks_queue, NUM_THREADS)
    for fn in glob.iglob(options.pattern):
        process_one_file(fn, mc_connections, tasks_queue, options.dry)
    for _ in workers:
        tasks_queue.put(Worker.SENTINEL)
    tasks_queue.join()


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
