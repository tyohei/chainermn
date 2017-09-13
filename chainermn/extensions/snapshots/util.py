import os
import shutil
import tempfile
import queue

from chainer.training import extension
from chainermn.extensions.snapshots import handler as handler_module


def snapshot(filename, outdir, handler):
    prefix = 'tmp' + filename
    fd, tmppath = tempfile.mkstemp(prefix=prefix, dir=outdir)
    try:
        handler.save(tmppath)
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
            return
        else:
            task()
            queue.task_done()


class SnapshotTask(object):

    def __init__(self, filename, outdir, handler):
        self._filename = filename
        self._outdir = outdir
        self._handler = handler

    def __call__(self):
        snapshot(self._filename, self._outdir, self._handler)


def generate_filename(comm=None, filename='snapshot_iter_{.updater.iteration}'):
    if comm is None:
        return filename
    else:
        return filename + '_{}'.comm.rank
