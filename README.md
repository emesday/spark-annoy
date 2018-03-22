[![Build Status](https://travis-ci.org/mskimm/ann4s.svg?branch=master)](https://travis-ci.org/mskimm/ann4s)

# Ann4s

-[x] Distributed Index Builds
    - handles 100M or more vectors
-[x] Annoy Compatible Binary
    - dumps index and items which is compatible with Annoy
-[ ] Queries are performed on persistence layers like HBase and RocksDB.
    - persist Index and Items in this layer
-[ ] CRUD supports
    - If `1` is the major build step. this step is minor build step to handle streaming data.
    - uses this as Lambda architecture.

# Distributed Builds & Dump to Annoy Compatible Binary

```
    val data: DataFrame = _
    val ann = new Annoy()
      .setIdCol("id")
      .setFeaturesCol("features")
      .setNumTrees(2)
      
    val annModel = ann.fit(data)
    
    annModel.write.save("/path/to/save/spark-ml-model")
    
    annModel.writeAnnoyBinary("/path/to/save/annoy-compatible-binary")
```

# Queries on Persistence Layers
TBD

# CRUD supports
TBD

# Deprecated API (tag v0.0.6)
```

# Spark code example (with DataFrame-based API)

## Item similarity computation
```scala
val dataset: DataFrame = ??? // your dataset

val alsModel: ALSModel = new ALS()
  .fit(dataset)

val annoyModel: AnnoyModel = new Annoy()
  .setDimension(alsModel.rank)
  .fit(alsModel.itemFactors)

val result: DataFrame = annoyModel
  .setK(10) // find 10 neighbors
  .transform(alsModel.itemFactors)

result.show()
```      

The `result.show()` shows

```
+---+--------+-----------+
| id|neighbor|   distance|
+---+--------+-----------+
|  0|       0|        0.0|
|  0|      50|0.014339785|
...
|  1|       1|        0.0|
|  1|      36|0.011467933|
...
+---+--------+-----------+
```

 - For more information of ALS see this [link](http://spark.apache.org/docs/2.0.0/ml-collaborative-filtering.html)
 - Working example is at 'src/test/scala/ann4s/spark/AnnoySparkSpec.scala'

```
resolvers += Resolver.bintrayRepo("mskimm", "maven")

libraryDependencies += "com.github.mskimm" %% "ann4s" % "0.0.6"
```
 - `0.0.6` is built with Apache Spark 1.6.2
 

# References
 - https://github.com/spotify/annoy : native implementation with serveral bindings like Python
 - https://github.com/pishen/annoy4s : Scala wrapper using JNA
 - https://github.com/spotify/annoy-java : Java implementation

