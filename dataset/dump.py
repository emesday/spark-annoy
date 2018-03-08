import sys
import h5py
import numpy as np

dataset = h5py.File(sys.argv[1], 'r')

for key, dtype in [('train', 'float32'), ('test', 'float32'), ('neighbors', 'int32'), ('distances', 'float32')]:
    arr = np.array(dataset[key], dtype=dtype)
    print(key, dtype, arr.shape)
    arr.tofile(key + '.bin')
