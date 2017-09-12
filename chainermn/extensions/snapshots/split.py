import re
import collections
from pympler import asizeof

from chainermn.extensions.snapshots import util

"""THIS FILE IS NOT COMPLETED!
"""


def split(comm):

    def split(filename, outdir, handler):
        target = handler.target
        participants = [x for x in range(0, comm.size)]
        root = participants[0]
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
        filename = filename + '_{}'.format()
        util.snapshot(filename, outdir, handler)
