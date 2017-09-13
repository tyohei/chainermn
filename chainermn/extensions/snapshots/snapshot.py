from chainer.training import extension
from chainermn.extensions.snapshots import handler as handler_module
from chainermn.extensions.snapshots import writer as writer_module
from chainermn.extensions.snapshots import condition as condition_module

class Snapshot(extension.Extension):

    def __init__(self,
                 target=None,
                 condition=condition_module.Everyone(),
                 writer=writer_module.SimpleWriter(),
                 handler=handler_module.NpzSerializerHandler(),
                 filename='snapshot_iter_{.updater.iteration}',
                 trigger=(1, 'epoch'),
                 priority=-100):
        self._target = target
        if callable(filename):
            self._filename = filename()
        else:
            self._filename = filename
        self.condition = condition
        self.writer = writer
        self.handler = handler
        self.trigger = trigger
        self.priority = priority

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
