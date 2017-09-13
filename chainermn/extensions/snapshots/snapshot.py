from chainer.training import extension
from chainermn.extensions.snapshots import handler as handler_module
from chainermn.extensions.snapshots import writer as writer_module
from chainermn.extensions.snapshots import condition as condition_module

class Snapshot(extension.Extension):
    """Takes a snapshot.

    Args:
        target: Object to serialize. If not specified, it will
            be trainer object.
        comm: ChainerMN communicator. It is only used for adding a rank
            to the snapshot filename.
        condition: Callable object. It need to return boolean in its call.
            It takes two arguments: the trainer object and this snapshot
            extension object. If its return True the snapshot will be
            done. If not it will be skipped.
        writer: Writer object.
        filename (str): Name of the file into which the object is serialized.

    """

    def __init__(self,
                 target=None,
                 comm=None,
                 condition=condition_module.Everyone(),
                 writer=writer_module.SimpleWriter(),
                 handler=handler_module.NpzSerializerHandler(),
                 filename='snapshot_iter_{.updater.iteration}'):
        self._target = target
        if comm is not None:
            self._filename = filename + '_{}'.format(comm.rank)
        else:
            self._filename = filename
        self.condition = condition
        self.writer = writer
        self.handler = handler

    def __call__(self, trainer):
        if self.condition(trainer, self):
            self._target = trainer if self._target is None else self._target
            self.handler.serialize(self._target)
            filename = self._filename.format(trainer)
            outdir = trainer.out
            self.writer.write(filename, outdir, self.handler)

    def finalize(self):
        if hasattr(self.writer, 'finalize'):
            self.writer.finalize()
