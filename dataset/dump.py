import sys
import h5py
import numpy as np

train = h5py.File(sys.argv[1], 'r')['train']
arr = np.array(train, dtype='f4')

# used big-endian for Java
with open('train.bin', 'wb') as f:
    np.array(train.shape, dtype='int32').astype('>i4').tofile(f)
    arr.astype('>f4').tofile(f)
