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
from collections import defaultdict
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
MEMC_TIMEOUT = 3

STATUS_ERR = 0
STATUS_OK = 1

AppsInstalled = collections.namedtuple(
    "AppsInstalled",
    ["dev_type", "dev_id", "lat", "lon", "apps"]
)

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
        return memcache.Client([addr], socket_timeout=MEMC_TIMEOUT)

    mc_connections = {}
    for dev_type in ['idfa', 'gaid', 'adid', 'dvid']:
        addr = getattr(options, dev_type)
        mc_connections[dev_type] = McConnection(addr, connect(addr), threading.Lock())

    return mc_connections


def get_mc_connection(mc_connections, dev_type):
    mc_connection = mc_connections.get(dev_type)
    if not mc_connection:
        raise RuntimeError("Unknow device type: %s", dev_type)

    return mc_connection


class Worker(threading.Thread):

    SENTINEL = 'quit'

    def __init__(self, queue):
        super(Worker, self).__init__()
        self.queue = queue
        self.executor = Executor()

    def run(self):
        while True:
            task = self.queue.get()
            self.queue.task_done()
            if isinstance(task, str) and task == self.SENTINEL:
                break
            self.executor.execute(task)


class Executor(object):
    MAX_RETRIES = 5

    def execute(self, task):
        logging.debug('run in thread %s: %s', threading.currentThread().ident, self)
        if task.dry_run:
            task.results_list.append(STATUS_OK)
            return

        try:
            with task.mc_connection.lock:
                self._set_mc_multi_value()
            task.results_list.extend([STATUS_OK] * len(task.batch))
        except Exception, e:
            logging.exception("Cannot write to memc %s: %s", task.mc_connection.addr, e)
            task.results_list.extend([STATUS_ERR] * len(task.batch))

    @retry_if_fails(MAX_RETRIES)
    def _set_mc_multi_value(self, task):
        multi_value = {
            key: protobuf_val.SerializeToString() for key, protobuf_val in task.batch.items()
        }
        task.mc_connection.client.set_multi(multi_value)


class UploadTask(object):
    def __init__(self, mc_connection, batch, results_list, dry_run=False):
        self.mc_connection = mc_connection
        self.batch = batch
        self.results_list = results_list
        self.dry_run = dry_run

    def __repr__(self):
        s = "batch: %s\n" % self.mc_connection.addr
        for key, protobuf_val in self.batch.items():
            s += "\t%s -> %s\n" % (key, str(protobuf_val).replace("\n", " "))


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


def parse_appsinstalled(line):
    line = line.strip()
    if not line:
        return

    err = ValueError('failed to parse')

    line_parts = line.strip().split("\t")
    if len(line_parts) < 5:
        raise err
    dev_type, dev_id, lat, lon, raw_apps = line_parts
    if not dev_type or not dev_id:
        raise err

    try:
        apps = [int(a.strip()) for a in raw_apps.split(",")]
    except ValueError:
        apps = [int(a.strip()) for a in raw_apps.split(",") if a.isidigit()]
        logging.info("Not all user apps are digits: `%s`" % line)

    try:
        lat, lon = float(lat), float(lon)
    except ValueError:
        logging.info("Invalid geo coords: `%s`" % line)
        raise err

    return AppsInstalled(dev_type, dev_id, lat, lon, apps)


def log_err_stat(fn, results_list):
    processed = results_list.count(STATUS_OK)
    errors = results_list.count(STATUS_ERR)
    err_rate = float(errors) / processed
    if err_rate < NORMAL_ERR_RATE:
        logging.info("Acceptable error rate (%s). Successfull load %s", err_rate, fn)
    else:
        logging.error("High error rate (%s > %s). Failed load %s", err_rate, NORMAL_ERR_RATE, fn)


def pop_batch(batches, batch_size):
    for dev_type, batch in list(batches.items()):
        if len(batch) >= batch_size:
            return dev_type, batches.pop(dev_type)

    return None, None


def process_one_file(fn, mc_connections, tasks_queue, dry_run):
    results_list = []
    logging.info('Processing %s', fn)
    fd = gzip.open(fn)
    batches = defaultdict(dict)

    for line in fd:
        try:
            appsinstalled = parse_appsinstalled(line)
            protobuf_struct = make_protobuf_struct(appsinstalled)
            key = "%s:%s" % (appsinstalled.dev_type, appsinstalled.dev_id)
            batches[appsinstalled.dev_type][key] = protobuf_struct
        except Exception as e:
            logging.error(e)
            results_list.append(STATUS_ERR)

        dev_type, batch = pop_batch(batches, BATCH_SIZE)

        if not batch:
            continue

        mc_connection = get_mc_connection(mc_connections, dev_type)
        tasks_queue.put(
            UploadTask(mc_connection, batch=batch, results_list=results_list, dry_run=dry_run))

    # add tasks from last iterations
    while True:
        dev_type, batch = pop_batch(batches, 0)
        if not batch:
            break
        mc_connection = get_mc_connection(mc_connections, dev_type)
        tasks_queue.put(
            UploadTask(mc_connection, batch=batch, results_list=results_list, dry_run=dry_run))

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
