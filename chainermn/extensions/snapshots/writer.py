import threading
import multiprocessing
import queue

from chainermn.extensions.snapshots import util

class Writer(object):

    def __init__(self):
        pass

    def write(self, filename, outdir, handler):
        pass


class SimpleWriter(Writer):

    def write(self, filename, outdir, handler):
        util.snapshot(filename, outdir, handler.save)


class ThreadWriter(Writer):

    def write(self, filename, outdir, handler):
        thread = threading.Thread(target=util.snapshot,
                                  args=(filename, outdir, handler.save))
        thread.start()


class ProcessWriter(Writer):

    def write(self, filename, outdir, handler):
        process = multiprocessing.Process(target=util.snapshot,
                                          args=(filename, outdir, handler.save))
        process.start()


class QueueThreadWriter(Writer):
    
    def initialize(self, snapshot, trainer):
        self._queue = queue.Queue()
        self._consumer = threading.Thread(target=util.snapshot_consumer,
                                          args=(self._queue,))
        self._consumer.start()

    def write(self, filename, outdir, handler):
        self._queue.put(util.SnapshotTask(filename, outdir, handler.save))

    def finalize(self, snapshot):
        self._queue.put(None)
        self._queue.join()
        self._consumer.join()


class QueueProcessWriter(Writer):
    
    def initialize(self, snapshot, trainer):
        self._queue = multiprocessing.JoinableQueue()
        self._consumer = multiprocessing.Process(target=util.snapshot_consumer,
                                                 args=(self._queue,))
        self._consumer.start()

    def write(self, filename, outdir, handler):
        self._queue.put(util.SnapshotTask(filename, outdir, handler.save))

    def finalize(self, snapshot):
        self._queue.put(None)
        self._queue.join()
        self._consumer.join()
