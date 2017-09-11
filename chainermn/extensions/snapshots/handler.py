import numpy
import h5py
import tempfile

from chainer.serializers import npz
from chainer.serializers import hdf5

class SerializerHandler(object):

    def serialize(self, target):
        raise NotImplementedError

    def save(self, filename, **kwds):
        raise NotImplementedError


class NpzSerializerHandler(SerializerHandler):

    def serialize(self, target):
        self.serializer = npz.DictionarySerializer()
        self.serializer.save(target)
        self.target = self.serializer.target

    def save(self, filename, compression=True):
        with open(filename, 'wb') as f:
            if compression:
                numpy.savez_compressed(f, **self.target)
            else:
                numpy.savez(f, **self.target)


class HDF5SerializerHandler(SerializerHandler):

    def serialize(self, target, compression=4):
        tmppath = 'tmp' + next(tempfile._get_candidate_names())
        self.group = h5py.File(tmppath)
        self.serializer = hdf5.HDF5Serializer(self.group, compression=compression)
        self.serializer.save(target)
        # Need to implement self.target

    def save(self, filename=None):
        if filename is not None:
            self.group.filename = filename
        self.group.close()
