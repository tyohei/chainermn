import re
import collections
from pympler import asizeof

from chainermn.extensions.snapshots import util

def split(comm):
    """
    ONLY NPZ FORMAT IS AVAILABLE FOR THIS FUNCTION
    """

    def split(filename, outdir, handler):
        target = handler.target
        participants = [x for x in range(0, comm.size)]
        srank = participants.index(comm.rank)
        root = participants[0]
        memsizes = {}
        local_target = {}
        pattern = 'updater/'
        # items() is defined in both npz and hdf5 format.
        for k, v in target.items():
            m = re.match(pattern, k)
            if m is not None:
                memsizes[k] = asizeof.asizeof(v)
            else:
                if srank == root:
                    local_target[k] = v
        memsizes = collections.OrderedDict(sorted(memsizes.items(),
                                                 key=lambda x: x[1]))
        for i in range(0, len(memsizes)):
            k, v = memsizes.popitem()
            if i % len(participants) == srank:
                local_target[k] = target[k]

        # Overwrite original target
        handler.target = local_target
        util.snapshot(filename, outdir, handler)

    return split
