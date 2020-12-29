import os
import h5py
from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors

dataset_list = ['glove-25-angular', 'nytimes-16-angular', 'fashion-mnist-784-euclidean']


def convert(spark, outpath, data):
    print('processing %s ... ' % outpath, end='')
    vectors = map(lambda x: (x[0], Vectors.dense(x[1])), enumerate(data))
    if not os.path.exists(outpath):
        spark.createDataFrame(vectors).write.parquet(outpath)

    expected = len(data)
    actual = spark.read.parquet(outpath).count()

    if expected != actual:
        print('ERROR: expected: %s, actual: %s' % (expected, actual))
    else:
        print('done')


if __name__ == '__main__':
    spark = SparkSession.builder.master('local[*]').config("spark.driver.memory", "10g").getOrCreate()

    for dataset in dataset_list:
        path = 'test/%s.hdf5' % dataset
        if not os.path.exists(path):
            print('launch dev/accuracy_test.py first')
        else:
            dataset_f = h5py.File(path, 'r')
            for key in dataset_f:
                outpath = 'test/parquet/%s/%s' % (dataset, key)
                convert(spark, outpath, dataset_f[key])
