import threading
import queue

from chainermn.extensions.snapshots import util

class MNSnapshotThread(util.SnapshotThread):

    def __init__(self, comm, rankdist, target=None, savetype='npz',
                 filename='snapshot_iter_{.updater.iteration}', trigger=(1, 'epoch'),
                 priority=-100):
        super(MNSnapshotThread, self).__init__(target, savetype, filename, trigger, priority)
        self._comm = comm
        self._rankdist = rankdist
        self._turn = 0

    def __call__(self, trainer):
        if self._comm.rank in self._rankdist[self._turn]:
            super(MNSnapshotThread, self).__call__(trainer)
        self._turn = (self._turn + 1) % len(self._rankdist)


class MNSnapshotQueue(util.SnapshotQueue):

    def __init__(self, comm, rankdist, target=None, savetype='npz',
                 filename='snapshot_iter_{.updater.iteration}', trigger=(1, 'epoch'),
                 priority=-100):
        super(MNSnapshotQueue, self).__init__(target, savetype, filename, trigger, priority)

        self._comm = comm
        self._rankdist = rankdist
        self._turn = 0

    def __call__(self, trainer):
        if self._comm.rank in self._rankdist[self._turn]:
            super(MNSnapshotQueue, self).__call__(trainer)
        self._turn = (self._turn + 1) % len(self._rankdist)

    def finalize(self):
        super(MNSnapshotQueue, self).finalize()
