class Condition(object):

    def __init__(self):
        pass

    def __call__(self, trainer, snapshot):
        pass


class Everyone(Condition):

    def __init__(self):
        pass

    def __call__(self, trainer, snapshot):
        return True


class Rotation(Condition):

    def __init__(self, comm, rankdist):
        self._comm = comm
        self._rankdist = rankdist
        self._turn = 0

    def __call__(self, trainer, snapshot):
        if self._comm.rank in self._rankdist[self._turn]:
            ret = True
        else:
            ret = False
        self._turn = (self._turn + 1) % len(self._rankdist)
        return ret
