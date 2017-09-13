import threading
import multiprocessing
import queue

from chainermn.extensions.snapshots import util

class Writer(object):

    def __init__(self):
        pass

    def write(self, filename, outdir, handler):
        pass

    def finalize(self):
        pass


class SimpleWriter(Writer):

    def __init__(self, func=util.snapshot):
        self._func = func

    def write(self, filename, outdir, handler):
        self._func(filename, outdir, handler)


class ThreadWriter(Writer):

    def __init__(self, func=util.snapshot):
        self._func = func

    def write(self, filename, outdir, handler):
        thread = threading.Thread(target=self._func,
                                  args=(filename, outdir, handler))
        thread.start()


class ProcessWriter(Writer):

    def __init__(self, func=util.snapshot):
        self._func = func

    def write(self, filename, outdir, handler):
        process = multiprocessing.Process(target=self._func,
                                          args=(filename, outdir, handler))
        process.start()


class QueueThreadWriter(Writer):
    
    def __init__(self, func=util.snapshot_consumer, task=util.SnapshotTask):
        self._task = task
        self._queue = queue.Queue()
        self._consumer = threading.Thread(target=func,
                                          args=(self._queue,))
        self._consumer.start()

    def write(self, filename, outdir, handler):
        self._queue.put(self._task(filename, outdir, handler))

    def finalize(self):
        self._queue.put(None)
        self._queue.join()
        self._consumer.join()


class QueueProcessWriter(Writer):
    
    def __init__(self, func=util.snapshot_consumer, task=util.SnapshotTask):
        self._task = task
        self._queue = multiprocessing.JoinableQueue()
        self._consumer = multiprocessing.Process(target=func,
                                                 args=(self._queue,),
                                                 daemon=True)
        self._consumer.start()

    def write(self, filename, outdir, handler):
        self._queue.put(self._task(filename, outdir, handler))

    def finalize(self):
        self._queue.put(None)
        self._queue.join()
        self._consumer.join()
