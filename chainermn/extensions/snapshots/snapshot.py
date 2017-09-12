from chainer.training import extension
from chainermn.extensions.snapshots import handler as handler_module
from chainermn.extensions.snapshots import writer as writer_module
from chainermn.extensions.snapshots import condition as condition_module

class Snapshot(extension.Extension):

    def __init__(self,
                 target=None,
                 condition=condition_module.Everyone(),
                 writer=writer_module.SimpleWriter(),
                 savetype='npz',
                 filename='snapshot_iter_{.updater.iteration}',
                 trigger=(1, 'epoch'),
                 priority=-100):
        self._target = target
        self._condition = condition
        self._writer = writer
        self._savetype = savetype
        self._filename = filename
        self.trigger = trigger
        self.priority = priority

    def __call__(self, trainer):
        if self._condition(trainer, self):
            self._target = trainer if self._target is None else self._target
            self._handler = self.create_handler()
            self._handler.serialize(self._target)
            filename = self._filename.format(trainer)
            outdir = trainer.out
            self._writer.write(filename, outdir, self._handler)

    def create_handler(self):
        if self._savetype == 'npz':
            handler = handler_module.NpzSerializerHandler()
        elif self._savetype == 'hdf5':
            handler = handler_module.HDF5SerializerHandler()
        else:
            raise NotImplementedError
        return handler

    def initialize(self, trainer):
        if hasattr(self._writer, 'initialize'):
            self._writer.initialize(self, trainer)

    def finalize(self):
        if hasattr(self._writer, 'finalize'):
            self._writer.finalize(self)
