import os
import shutil
import tempfile
import threading
import multiprocessing
import queue

from chainer.training import extension
from chainermn.extensions.snapshots import handler as handler_module

class Snapshot(extension.Extension):

    def __init__(self, target=None, savetype='npz', filename='snapshot_iter_{.updater.iteration}',
                 trigger=(1, 'epoch'), priority=-100):
        self._target = target
        self._savetype = savetype
        self._filename = filename
        self.trigger = trigger
        self.prority = priority

    def __call__(self, trainer):
        target = trainer if self._target is None else self._target
        handler = self.create_handler()
        handler.serialize(target)
        filename = self._filename.format(trainer)
        outdir = trainer.out
        snapshot(filename, outdir, handler.save)

    def create_handler(self):
        if self._savetype == 'npz':
            handler = handler_module.NpzSerializerHandler()
        elif self._savetype == 'hdf5':
            handler = handler_module.HDF5SerializerHandler()
        else:
            raise NotImplementedError
        return handler


class SnapshotThread(Snapshot):

    def __init__(self, target=None, savetype='npz', filename='snapshot_iter_{.updater.iteration}',
                 trigger=(1, 'epoch'), priority=-100):
        super(SnapshotThread, self).__init__(target, savetype, filename, trigger, priority)

    def __call__(self, trainer):
        target = trainer if self._target is None else self._target
        handler = self.create_handler()
        handler.serialize(target)
        filename = self._filename.format(trainer)
        outdir = trainer.out
        thread = threading.Thread(target=snapshot,
                                  args=(filename, outdir, handler.save))
        thread.start()


class SnapshotProcess(Snapshot):

    def __init__(self, target=None, savetype='npz', filename='snapshot_iter_{.updater.iteration}',
                 trigger=(1, 'epoch'), priority=-100):
        super(SnapshotProcess, self).__init__(target, savetype, filename, trigger, priority)

    def __call__(self, trainer):
        target = trainer if self._target is None else self._target
        handler = self.create_handler()
        handler.serialize(target)
        filename = self._filename.format(trainer)
        outdir = trainer.out
        process = multiprocessing.Process(target=snapshot,
                                          args=(filename, outdir, handler.save))
        process.start()


class SnapshotQueue(Snapshot):

    def __init__(self, target=None, savetype='npz', qtype='process', filename='snapshot_iter_{.updater.iteration}',
                 trigger=(1, 'epoch'), priority=-100):
        super(SnapshotQueue, self).__init__(target, savetype, filename, trigger, priority)
        if qtype == 'process':
            self._queue = multiprocessing.JoinableQueue()
            self._consumer = multiprocessing.Process(target=snapshot_consumer,
                                                     args=(self._queue,))
        elif qtype == 'thread':
            self._queue = queue.Queue()
            self._consumer = threading.Thread(target=snapshot_consumer,
                                              args=(self._queue,))
        else:
            raise NotImplementedError
        self._consumer.start()

    def __call__(self, trainer):
        target = trainer if self._target is None else self._target
        handler = self.create_handler()
        handler.serialize(target)
        filename = self._filename.format(trainer)
        outdir = trainer.out
        self._queue.put(SnapshotTask(filename, outdir, handler.save))

    def finalize(self):
        self._queue.put(None)
        self._queue.join()
        self._consumer.join()


def snapshot(filename, outdir, savefun):
    prefix = 'tmp' + filename
    fd, tmppath = tempfile.mkstemp(prefix=prefix, dir=outdir)
    try:
        savefun(tmppath)
    except Exception:
        os.close(fd)
        os.remove(tmppath)
        raise
    os.close(fd)
    shutil.move(tmppath, os.path.join(outdir, filename))


def snapshot_consumer(queue):
    while True:
        task = queue.get()
        if task is None:
            queue.task_done()
            break
        else:
            task()
            queue.task_done()

class SnapshotTask(object):

    def __init__(self, filename, outdir, savefun):
        self._filename = filename
        self._outdir = outdir
        self._savefun = savefun

    def __call__(self):
        snapshot(self._filename, self._outdir, self._savefun)
