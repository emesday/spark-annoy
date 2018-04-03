import sys
import h5py
import numpy as np

train = h5py.File(sys.argv[1], 'r')['train']
arr = np.array(train, dtype='f4')

with open('../data/annoy/sample-glove-25-angular.txt', 'w') as f:
    for i, sample in enumerate(arr[:1000]):
        f.write(str(i) + '\t' + ','.join([str(x) for x in sample]) + '\n')

# used big-endian for Java
with open('train.bin', 'wb') as f:
    np.array(train.shape, dtype='int32').astype('>i4').tofile(f)
    arr.astype('>f4').tofile(f)
