import re
import collections
from pympler import asizeof

from chainermn.extensions.snapshots import util
from chainermn.extensions.snapshots import thread
from chainermn.extensions.snapshots import writers


# ONLY AVAILABLE AT NPZ FORMAt
class MNSplitThreadWriter(writers.ThreadWriter):

    def __init__(self, comm, participants):
        self._comm = comm
        self._participants = participants

    def write(self, filename, outdir, handler):
        target = handler.target
        root = self._participants[0]
        memsizes = {}
        pattern = 'updater/'
        # items() is defined in both npz and hdf5 format.
        for k, v in target.items():
            m = re.match(pattern, k)
            if m is not None:
                memsizes[k] = asizeof.asizeof(v)
            else:
                if srank == root:
                    local_target[k] = v
        memsizes = collections.OrderdDict(sorted(memsizes.items(),
                                                 key=lambda x: x[1]))
        for i in range(0, len(memsizes)):
            k, v = memsizes.popitem()
            if i % len(participants) == srank:
                local_target[k] = target[k]

        # Overwrite original target
        target = local_target

        super(MNSplitThreadWriter, self).__call__(filename, outdir, handler)
