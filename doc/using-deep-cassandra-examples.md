---
title: Using Stratio Deep with Cassandra (examples)
---

In the examples below, generic names have been used for the following objects:

-   TableEntity: class of the entity object
-   TableEntity.getKey(): getter for a column of the table bound by TableEntity

Replace them with your own object names before using these snippets.

-   [Creating a Deep Context](#creating-a-deep-context)
-   [Creating a Cassandra RDD](#creating-a-cassandra-rdd)
-   [Grouping Tuples](#grouping-tuples)
-   [Map and Reduce](#map-and-reduce)
-   [Writing to Cassandra](#writing-to-cassandra)
-   [Aggregation](#aggregation)

Creating a Deep Context
=======================

Local context
-------------

{% tabgroup %}
{% tab Scala %}
```scala
val cluster = "local"
val job = "myjobname"
val sparkHome = "/path/to/StratioDeep"
val jarList = Array("/path/to/myfirstjar.jar", "/path/to/mysecondjar.jar")

val deepContext: DeepSparkContext = new DeepSparkContext(cluster, job, sparkHome, jarList)
```
{% endtab %}
{% tab Java %}
```java
String cluster = "local";
String job = "myJobName";
String sparkHome = "/path/to/StratioDeep";
String[] jarList = {"/path/to/myfirstjar.jar","/path/to/mysecondjar.jar"};

// Creating the Deep Context where args are Spark Master and Job Name
DeepSparkContext deepContext = new DeepSparkContext(cluster, job, sparkHome, jarList);
```
{% endtab %}
{% endtabgroup %}

Cluster context
---------------

{% tabgroup %}
{% tab Scala %}
```scala
val cluster = "spark://hostname:port"
val job = "myjobname"

val deepContext: DeepSparkContext = new DeepSparkContext(cluster, job)

// Add jars to the context
deepContext.addJar("/path/to/jarfile.jar")
```
{% endtab %}
{% tab Java %}
```java
String cluster = "spark://hostname:port";
String job = "myJobName";

// Creating the Deep Context where args are Spark Master and Job Name
DeepSparkContext deepContext = new DeepSparkContext(cluster, job);

// Add jars to the context
deepContext.addJar("/path/to/jarfile.jar")
```
{% endtab %}
{% endtabgroup %}

Creating a Cassandra RDD
========================

Cell RDD
--------

A cell (or generic) RDD does not need an entity object to operate with Cassandra data. In this case, 
columns are bound to generic cells that include metadata along with the values.

{% tabgroup %}
{% tab Scala %}
```scala
// Creating the Deep Context
val deepContext: DeepSparkContext = new DeepSparkContext(cluster, job)

// Configuration and initialization
val config: ICassandraDeepJobConfig[Cells] = DeepJobConfigFactory.create()
    .host(cassandraHost).rpcPort(cassandraPort)
    .keyspace(keyspaceName).table(tableName)
    .initialize

// Creating the RDD
val rdd: CassandraRDD[Cells] = deepContext.cassandraGenericRDD(config)
```
{% endtab %}
{% tab Java %}
```java
// Creating the Deep Context
DeepSparkContext deepContext = new DeepSparkContext(cluster, job, sparkHome, jarList);

// Configuration and initialization
ICassandraDeepJobConfig<Cells> config = DeepJobConfigFactory.create()
        .host(cassandraHost).rpcPort(cassandraPort)
        .keyspace(keyspaceName).table(tableName)
        .initialize();

// Creating the RDD
CassandraJavaRDD rdd = deepContext.cassandraJavaRDD(config);
```
{% endtab %}
{% endtabgroup %}

Entity RDD
----------

When working with entity objects, an entity RDD must be used.

{% tabgroup %}
{% tab Scala %}
```scala
// Create the Deep Context
val deepContext: DeepSparkContext = new DeepSparkContext(cluster, job)

// Configure and initialize the RDD
val config = DeepJobConfigFactory.create(classOf[TableEntity])
                .host(cassandraHost).rpcPort(cassandraPort)
                .keyspace(keyspaceName).table(tableName)
                .initialize

// Create the RDD
val rdd: CassandraRDD[TableEntity] = deepContext.cassandraEntityRDD(config)
```
{% endtab %}
{% tab Java %}
```java
// Creating the Deep Context
DeepSparkContext deepContext = new DeepSparkContext(cluster, job);

// Create a configuration for the RDD and initialize it
ICassandraDeepJobConfig<TableEntity> config = DeepJobConfigFactory.create(TableEntity.class)
        .host(cassandraHost).rpcPort(cassandraPort)
        .keyspace(keyspaceName).table(tableName)
        .initialize();

// Creating the RDD
CassandraJavaRDD rdd = deepContext.cassandraJavaRDD(config);
```
{% endtab %}
{% endtabgroup %}

Grouping Tuples
===============

Using GroupBy
-------------

{% tabgroup %}
{% tab Scala %}
```scala
// grouping
val groups: RDD[(String, Iterable[TableEntity])] = rdd groupBy  {t:TableEntity => t.getKey}

// counting elements in groups
val counts: RDD[(String, Int)] = groups map {t:(String, Iterable[TableEntity]) => (t._1, t._2.size)} 

// fetching results
val result: Array[(String, Int)] = counts.collect()
```
{% endtab %}
{% tab Java %}
```java
// grouping
JavaPairRDD<String, Iterable<TableEntity>> groups = rdd.groupBy(new Function<TableEntity, String>() {
    @Override
    public String call(TableEntity tableEntity) throws Exception {
        return tableEntity.getKey();
    }
});

// counting elements in groups
JavaPairRDD<String,Integer> counts = groups.mapToPair(new PairFunction<Tuple2<String, Iterable<TableEntity>>, String, Integer>() {
    @Override
    public Tuple2<String, Integer> call(Tuple2<String, Iterable<TableEntity>> t) throws Exception {
        // I need to wrap the Iterable into a List to get its size
        return new Tuple2<String,Integer>(t._1(), Lists.newArrayList(t._2()).size());
    }
});

// fetching the results
List<Tuple2<String,Integer>> results = counts.collect();
```
{% endtab %}
{% endtabgroup %}

Using GroupByKey
----------------

{% tabgroup %}
{% tab Scala %}
```scala
// !!! IMPORTANT !!!
import org.apache.spark.SparkContext._ 

...

// creating a key-value pairs RDD
val pairsRDD: RDD[(String, TableEntity)] = rdd map { e: TableEntity => (e.getKey, e)}

// grouping by key
val groups: RDD[(String, Iterable[TableEntity])] = pairsRDD.groupByKey

// counting elements in groups
val counts: RDD[(String, Int)] = groups map {t:(String, Iterable[TableEntity]) => (t._1, t._2.size)}

// fetching results
val result: Array[(String, Int)] = counts.collect()
```
{% endtab %}
{% tab Java %}
```java
// creating a key-value pairs RDD
JavaPairRDD<String,TableEntity> pairsRDD = rdd.mapToPair(new PairFunction<TableEntity, String, TableEntity>() {
    @Override
    public Tuple2<String, TableEntity> call(TableEntity t) throws Exception {
        return new Tuple2<String,TableEntity>(t.getKey(),t);
    }
});

// grouping
JavaPairRDD<String, Iterable<TableEntity>> groups = pairsRDD.groupByKey();

// counting elements in groups
JavaPairRDD<String,Integer> counts = groups.mapToPair(new PairFunction<Tuple2<String, Iterable<TableEntity>>, String, Integer>() {
    @Override
    public Tuple2<String, Integer> call(Tuple2<String, Iterable<TableEntity>> t) throws Exception {
        // I need to wrap the Iterable into a List to get its size
        return new Tuple2<String, Integer>(t._1(), Lists.newArrayList(t._2()).size());
    }
});

// fetching results
List<Tuple2<String, Integer>> result = counts.collect();
```
{% endtab %}
{% endtabgroup %}

Map and Reduce
==============

{% tabgroup %}
{% tab Scala %}
```scala
// Important imports
import org.apache.spark.SparkContext._
import com.example.TableEntity

...

// Map stage: Getting key-value pairs from the RDD
val pairsRDD: RDD[(String, Int)] = rdd map {e:TableEntity => (e.getKey,1)}

// Reduce stage: counting rows
val counts: RDD[(String, Int)] = pairsRDD reduceByKey {_ + _}

// Fetching the results
val results: Array[(String, Int)] = counts.collect()
```
{% endtab %}
{% tab Java %}
```java
// Map stage: Getting key-value pairs from the RDD
JavaPairRDD<String, Integer> pairsRDD = rdd.mapToPair(new PairFunction<TableEntity, String, Integer>() {
    @Override
    public Tuple2<String, Integer> call(TableEntity t) throws Exception {
        return new Tuple2<String,Integer>(t.getKey(), 1);
    }
});

// Reduce stage: counting rows
JavaPairRDD<String, Integer> counts = pairsRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
    @Override
    public Integer call(Integer a, Integer b) {
        return a + b;
    }
});

// Fetching the results
List<Tuple2<String,Integer>> results = counts.collect();
```
{% endtab %}
{% endtabgroup %}

Writing to Cassandra
====================

Writing a Cell RDD
------------------

{% tabgroup %}
{% tab Scala %}
```scala
// --- INPUT RDD
val inputConfig = DeepJobConfigFactory.create()
    .host(cassandraHost).rpcPort(cassandraPort)
    .keyspace(inputKeyspaceName).table(inputTableName)
    .initialize

val inputRDD: CassandraRDD[Cells] = deepContext.cassandraGenericRDD(inputConfig)

val pairRDD: RDD[(String, Cells)] = inputRDD map {
    c:Cells => (c.getCellByName("columnName").getCellValue.asInstanceOf[String], c)
}

val numPerKey: RDD[(String, Integer)] = pairRDD.groupByKey
    .map { t:(String, Iterable[Cells]) => (t._1, t._2.size)}

// --- OUTPUT RDD
val outputConfig = DeepJobConfigFactory.createWriteConfig()
        .host(cassandraHost).rpcPort(cassandraPort)
        .keyspace(outputKeyspaceName).table(outputTableName)
        .createTableOnWrite(true)
        .initialize

val outputRDD: RDD[Cells] = numPerKey map { t: (String, Int) =>
    val c1 = Cell.create("primaryKeyColumnName", t._1, true, false);
    val c2 = Cell.create("otherColumnName", t._2);
    new Cells(c1, c2)
}

// Write to Cassandra
CassandraRDD.saveRDDToCassandra(outputRDD, outputConfig)
```
{% endtab %}
{% tab Java %}
```java
// --- INPUT RDD
ICassandraDeepJobConfig<Cells> inputConfig = DeepJobConfigFactory.create()
        .host(cassandraHost).rpcPort(cassandraPort)
        .keyspace(keyspaceName).table(inputTableName)
        .initialize();

CassandraJavaRDD<Cells> inputRDD = deepContext.cassandraJavaRDD(inputConfig);

JavaPairRDD<String,Cells> pairRDD = inputRDD.mapToPair(new PairFunction<Cells,String,Cells>() {
    @Override
    public Tuple2<String,Cells> call(Cells c) throws Exception {
        return new Tuple2<String, Cells>((String) c.getCellByName("columnName")
            .getCellValue(),c);
    }
});

JavaPairRDD<String,Integer> numPerKey = pairRDD.groupByKey()
        .mapToPair(new PairFunction<Tuple2<String, Iterable<Cells>>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<String, Iterable<Cells>> t) throws Exception {
                // I need to wrap the Iterable into a List to get its size
                return new Tuple2<String, Integer>(t._1(), Lists.newArrayList(t._2()).size());
            }
        });

// --- OUTPUT RDD
ICassandraDeepJobConfig<Cells> outputConfig = DeepJobConfigFactory.createWriteConfig()
        .host(cassandraHost).rpcPort(cassandraPort)
        .keyspace(keyspaceName).table(outputTableName)
        .createTableOnWrite(true)
        .initialize();

JavaRDD<Cells> outputRDD = numPerKey.map(new Function<Tuple2<String, Integer>, Cells>() {
    @Override
    public Cells call(Tuple2<String, Integer> t) throws Exception {
        Cell c1 = Cell.create("primaryKeyColumnName",t._1(),true,false);
        Cell c2 = Cell.create("otherColumn",t._2());
        return new Cells(c1, c2);
    }
});

// Write to Cassandra
CassandraRDD.saveRDDToCassandra(outputRDD, outputConfig);
```
{% endtab %}
{% endtabgroup %}

Writing an Entity RDD
---------------------

{% tabgroup %}
{% tab Scala %}
```scala
// --- INPUT RDD
val inputConfig = DeepJobConfigFactory.create(classOf[InputTableEntity])
    .host(cassandraHost).rpcPort(cassandraPort)
    .keyspace(inputKeyspaceName).table(inputTableName)
    .initialize

val inputRDD: CassandraRDD[InputTableEntity] = deepContext.cassandraEntityRDD(inputConfig)

val pairRDD: RDD[(String, InputTableEntity)] = inputRDD map {e:IntputTableEntity => (e.getKey, e)}

val numPerKey: RDD[(String, Int)] = pairRDD.groupByKey
    .map { t:(String, Iterable[InputTableEntity]) => (t._1, t._2.size)}

// --- OUTPUT RDD
val outputConfig = DeepJobConfigFactory.createWriteConfig(classOf[OutputTableEntity])
    .host(cassandraHost).rpcPort(cassandraPort)
    .keyspace(outputKeyspaceName).table(outputTableName)
    .initialize

val outputRDD: RDD[OutputTableEntity] = numPerKey map { t: (String, Int) => 
    new OutputTableEntity(t._1, t._2);
}

// Write to Cassandra
CassandraRDD.saveRDDToCassandra(outputRDD, outputConfig)
```
{% endtab %}
{% tab Java %}
```java
// --- INPUT RDD
ICassandraDeepJobConfig<InputEntity> inputConfig = DeepJobConfigFactory.create(InputEntity.class)
        .host(cassandraHost).rpcPort(cassandraPort)
        .keyspace(keyspaceName).table(inputTableName)
        .initialize();

CassandraJavaRDD<InputEntity> inputRDD = deepContext.cassandraJavaRDD(inputConfig);

JavaPairRDD<String,InputEntity> pairRDD = inputRDD.mapToPair(new PairFunction<InputEntity,String,InputEntity>() {
    @Override
    public Tuple2<String,InputEntity> call(InputEntity e) throws Exception {
        return new Tuple2<String, InputEntity>(e.getKey(),e);
    }
});

JavaPairRDD<String,Integer> numPerKey = pairRDD.groupByKey()
        .mapToPair(new PairFunction<Tuple2<String, Iterable<InputEntity>>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<String, Iterable<InputEntity>> t) throws Exception {
                // I need to wrap the Iterable into a List to get its size
                return new Tuple2<String, Integer>(t._1(), Lists.newArrayList(t._2()).size());
            }
        });

// --- OUTPUT RDD
ICassandraDeepJobConfig<OutputEntity> outputConfig = DeepJobConfigFactory.createWriteConfig(OutputEntity.class)
        .host(cassandraHost).rpcPort(cassandraPort)
        .keyspace(keyspaceName).table(outputTableName)
        .initialize();

JavaRDD<OutputEntity> outputRDD = numPerKey.map(new Function<Tuple2<String, Integer>, OutputEntity>() {
    @Override
    public OutputEntity call(Tuple2<String, Integer> t) throws Exception {
        OutputEntity e = new OutputEntity();
        e.setKey(t._1());
        e.setValue(t._2());
        return e;
    }
});

// Write to Cassandra
CassandraRDD.saveRDDToCassandra(outputRDD, outputConfig);
```
{% endtab %}
{% endtabgroup %}

Aggregation
===========

{% tabgroup %}
{% tab Scala %}
```scala
// grouping to get key-value pairs
val groups: RDD[(String,Int)] = rdd groupBy  {t:TableEntity => t.getKey}
    .map {t:(String, Iterable[TableEntity]) => (t._1, t._2.size)}

// aggregating
val (sumOfX, n, sumOfSquares): (Int,Int,Double) = groups.aggregate(0:Int,0:Int,0:Double)({
    (n:(Int,Int,Double), t:(String,Int)) =>
        (n._1 + t._2, n._2 + 1, n._3 + pow(t._2, 2))
},{ (u:(Int,Int,Double), v:(Int,Int,Double)) =>
    (u._1 + v._1, u._2 + v._2, u._3 + v._3)
})

// computing stats
val avg: Double = sumOfX.toDouble / n.toDouble
val variance: Double = (sumOfSquares.toDouble / n.toDouble) - pow(avg,2)
val stddev: Double = sqrt(variance)
```
{% endtab %}
{% tab Java %}
```java
// grouping to get key-value pairs
JavaPairRDD<String,Integer> groups = rdd.groupBy(new Function<TableEntity, String>() {
    @Override
    public String call(TableEntity tableEntity) throws Exception {
        return tableEntity.getKey();
    }
}).mapToPair(new PairFunction<Tuple2<String, Iterable<TableEntity>>, String, Integer>() {
    @Override
    public Tuple2<String, Integer> call(Tuple2<String, Iterable<TableEntity>> t) throws Exception {
        return new Tuple2<String, Integer>(t._1(), Lists.newArrayList(t._2()).size());
    }
});

// aggregating
Double zero = new Double(0);
Tuple3<Double, Double, Double> initValues = new Tuple3<Double, Double, Double>(zero,zero,zero);
Tuple3<Double, Double, Double> results = groups.aggregate(initValues,
        new Function2<Tuple3<Double, Double, Double>, Tuple2<String, Integer>, Tuple3<Double, Double, Double>>() {
            @Override
            public Tuple3<Double, Double, Double> call(Tuple3<Double, Double, Double> n, Tuple2<String, Integer> t) throws Exception {
                Double sumOfX = n._1() + t._2();
                Double numOfX = n._2() + 1;
                Double sumOfSquares = n._3() + Math.pow(t._2(),2);
                return new Tuple3<Double, Double, Double>(sumOfX, numOfX, sumOfSquares);
            }
        }, new Function2<Tuple3<Double, Double, Double>, Tuple3<Double, Double, Double>, Tuple3<Double, Double, Double>>() {
            @Override
            public Tuple3<Double, Double, Double> call(Tuple3<Double, Double, Double> a, Tuple3<Double, Double, Double> b) throws Exception {
                Double sumOfX = a._1() + b._1();
                Double numOfX = a._2() + b._2();
                Double sumOfSquares = a._3() + b._3();
                return new Tuple3<Double, Double, Double>(sumOfX,numOfX, sumOfSquares);
            }
        }
);

// computing stats
Double sumOfX = results._1();
Double numOfX = results._2();
Double sumOfSquares = results._3();

Double avg = sumOfX / numOfX;
Double variance = (sumOfSquares / numOfX) - Math.pow(avg,2);
Double stddev = Math.sqrt(variance);
```
{% endtab %}
{% endtabgroup %}
