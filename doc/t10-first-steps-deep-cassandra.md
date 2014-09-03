---
title: First Steps with Stratio Deep and Cassandra
---

StratioDeep is an integration layer between Spark, a distributed computing framework and Cassandra. 
[Cassandra](http://cassandra.apache.org/ "Apache Cassandra website") (C\*) is a NoSQL 
distributed and eventually consistent database based on a P2P model. It provides a column-oriented 
data model richer than typical key/value systems. [Spark](http://spark.apache.org/ "Spark website") is 
a fast and general-purpose cluster computing system that can run applications up to 100 times faster 
than Hadoop. It processes data using Resilient Distributed Datasets (RDDs) allowing storage of intermediate 
results in memory for future processing reuse. Spark applications are written in 
[Scala](http://www.scala-lang.org/ "The Scala programming language site"), a popular functional language 
for the Java Virtual Machine (JVM). Integrating Cassandra and Spark gives us a system that combines the 
best of both worlds opening to Cassandra the possibility of solving a wide range of new use cases. Stratio 
Deep provides a seamless extension to the Cassandra Query Language (CQL) that translates custom CQL queries 
to Spark jobs and which delegates to the former the complexity of distributing the query itself over the 
underlying cluster, moreover its Java friendly API allows developers to access data using custom serializable 
entity objects.

Summary
=======

This tutorial shows how Stratio Deep can be used to perform simple to complex queries and calculations on 
data stored in a Cassandra cluster. You will learn:

-   How to use the Stratio Deep interactive shell.
-   How to create a RDD from Cassandra and perform operations on the data.
-   How to write data from a RDD to Cassandra.

Table of Contents
=================

-   [Summary](#summary)
-   [Before you start](#before-you-start)
    -   [Prerequisites](#prerequisites)
    -   [Configuration](#configuration)
    -   [Notes](#notes)
        -   [Changes in Cassandra version 2](#changes-in-cassandra-version-2)
        -   [Method invocation syntax in Scala](#method-invocation-syntax-in-scala)
-   [Creating the keyspace and table in Cassandra](#creating-the-keyspace-and-table-in-cassandra)
    -   [Step 1: Creating the keyspace](#step-1-creating-the-keyspace)
    -   [Step 2: Creating the table schemas](#step-2-creating-the-table-schemas)
-   [Loading the dataset](#loading-the-dataset)
    -   [Alt 1: Using SSTables](#alt-1-using-sstables)
    -   [Alt 2: Using the CSV file](#alt-2-using-the-csv-file)
    -   [Alt 3: Using the JSON dump](#alt-3-using-the-json-dump)
-   [Using the Stratio Deep Shell](#using-the-stratio-deep-shell)
    -   [Step 1: Creating a RDD](#step-1-creating-a-rdd)
    -   [Step 2: Filtering data](#step-2-filtering-data)
    -   [Step 3: Caching data](#step-3-caching-data)
    -   [Step 4: Grouping data](#step-4-grouping-data)
    -   [Step 5: Writing the results to Cassandra](#step-5-writing-the-results-to-cassandra)
-   [Where to go from here](#where-to-go-from-here)
-   [Troubleshooting](#troubleshooting)
    -   [“TSocket read 0 bytes” when bulk loading data into Cassandra](#tsocket-read-0-bytes-when-bulk-loading-data-into-cassandra)
    -   [NullPointer exception when writing to Cassandra](#nullpointer-exception-when-writing-to-cassandra)

Before you start
================

Prerequisites
-------------

This tutorial assumes the reader has installed Cassandra and Stratio Deep on a single machine. Follow the instructions of the 
[Getting Started](/getting-started.html "Getting Started") page if you need to install the software.

Regarding programming skills, basic knowledge of CQL (or a SQL like language), Java and/or Scala are required.

Configuration
-------------

Read this section carefully if you are on a multi nodes Cassandra cluster, otherwise you can skip it. Make sure 
the cassandra.yaml configuration file is in the classpath and that the following properties are configured properly:

-   cluster_name
-   listen_address
-   storage_port
-   rpc_address
-   rpc_port
-   seed_provider, and seeds

Information about how to set these parameters can be found in the 
[Cassandra documentation](http://www.datastax.com/documentation/cassandra/2.0/webhelp/index.html#cassandra/configuration/../../cassandra/configuration/configCassandra_yaml_r.html "Cassandra Configuration Documentation").

Notes
-----

### Changes in Cassandra version 2

Version 2 of Cassandra comes with many improvements along with a revised vocabulary. In this tutorial, the most 
recent vocabulary is used. The few changes are outlined in the table below:

|Version 1|Version 2|
|:--------|:--------|
|Column Family|Table|
|Column|Cell|
|Row|Partition|

For more information about improvements and changes in Cassandra version 2, please refer to 
[http://www.datastax.com/documentation/articles/cassandra/cassandrathenandnow.html](http://www.datastax.com/documentation/articles/cassandra/cassandrathenandnow.html "Cassandra then and now Paper")

### Method invocation syntax in Scala

The pieces of code included in this document have been written following the 
[Scala Style Guide](http://docs.scala-lang.org/style/ "The Style Guide at Scala website"). 
Special attention has been put on the method invocation syntax which follows Java convention in 
most cases. However it may differ depending on the order and arity of the invoked method. For more 
details, refer to the 
“[Method Invocation](http://docs.scala-lang.org/style/method-invocation.html "Method Invocation section of the Scala Style Guide")” 
section of the Scala Style Guide.

Creating the keyspace and table in Cassandra
============================================

For this tutorial we will need some data to operate on. The dataset provided in this tutorial contains data 
gathered by a web crawler: url, date, page contents, etc. Before loading the data, the destination keyspace 
and table must be created. If Cassandra is not running yet, launch it using the following command:

```shell-session
$ /PATH/TO/CASSANDRA/bin/cassandra -f
```

Step 1: Creating the keyspace
-----------------------------

Launch the CQL shell:

```shell-session
$ /PATH/TO/CASSANDRA/bin/cqlsh
```

The shell will start displaying a welcome message and the prompt:

```shell-session
Connected to Test Cluster at localhost:9160.
[cqlsh 4.1.1 | Cassandra 2.0.82 | CQL spec 3.1.1 | Thrift protocol 19.39.0]
Use HELP for help.
```

Create the keyspace:

```shell-session
cqlsh> CREATE KEYSPACE crawler WITH replication = {
      'class': 'SimpleStrategy',
      'replication_factor': '1'
};
```

If the keyspace has been created successfully, no feedback will be shown. To get a list of existing keyspaces, 
use the following command:

```shell-session
cqlsh> describe keyspaces
```

Unless you have previously created others keyspaces, you should see a list similar to the one below:

```shell-session
cqlsh> describe keyspaces
system crawler system_traces
```

Step 2: Creating the table schemas
----------------------------------

To create the schema for the table “Page”, we will use the CQL script provided with this tutorial: 
[table-Page-create.cql](resources/table-Page-create.cql "CQL script to create the table "Page""). 
If you prefer so, you can create the schema manually by entering the statements in the CQL shell. Source 
the script from the CQL shell:

```shell-session
cqlsh> use crawler;
cqlsh:crawler> SOURCE '/PATH/TO/SCRIPT/table-Page-create.cql';
```

Check the tables have been created correctly:

```shell-session
 cqlsh:crawler> describe table "Page";
```

Notice the double quotes surrounding the table name to force a case sensitive interpretation of the table 
name. Without quotes, it would be interpreted as “page” instead of “Page”. Once the creation script has 
been run, the “describe table” command should produce the following output:

```shell-session
CREATE TABLE "Page" (
 key text,
 "___class" text,
 charset text,
 content text,
 "domainName" text,
 "downloadTime" bigint,
 "enqueuedForTransforming" bigint,
 etag text,
 "firstDownloadTime" bigint,
 "lastModified" text,
 "responseCode" varint,
 "responseTime" bigint,
 "timeTransformed" bigint,
 title text,
 url text,
 PRIMARY KEY (key)
) WITH
 bloom_filter_fp_chance=0.010000 AND
 caching='KEYS_ONLY' AND
 comment='' AND
 dclocal_read_repair_chance=0.000000 AND
 gc_grace_seconds=864000 AND
 index_interval=128 AND
 read_repair_chance=0.100000 AND
 replicate_on_write='true' AND
 populate_io_cache_on_flush='false' AND
 default_time_to_live=0 AND
 speculative_retry='99.0PERCENTILE' AND
 memtable_flush_period_in_ms=0 AND
 compaction={'class': 'SizeTieredCompactionStrategy'} AND
 compression={'sstable_compression': 'LZ4Compressor'};
```

Then same steps will be repeated to create the schema for the table “listdomains”, using the 
[table-listdomains-create.cql](resources/table-listdomains-create.cql "CQL script to create the table "listdomains"") 
script: Source the script from the CQL shell:

```shell-session
cqlsh:crawler> SOURCE '/PATH/TO/SCRIPT/table-listdomains-create.cql';
```

Then check that the table has been created correctly:

```shell-session
cqlsh:crawler> describe table listdomains;
```

Once the creation script has been run, the “describe table” command should produce the following output:

```shell-session
CREATE TABLE listdomains (
 domain text,
 num_pages int,
 PRIMARY KEY (domain)
) WITH
 bloom_filter_fp_chance=0.010000 AND
 caching='KEYS_ONLY' AND
 comment='' AND
 dclocal_read_repair_chance=0.000000 AND
 gc_grace_seconds=864000 AND
 index_interval=128 AND
 read_repair_chance=0.100000 AND
 replicate_on_write='true' AND
 populate_io_cache_on_flush='false' AND
 default_time_to_live=0 AND
 speculative_retry='99.0PERCENTILE' AND
 memtable_flush_period_in_ms=0 AND
 compaction={'class': 'SizeTieredCompactionStrategy'} AND
 compression={'sstable_compression': 'SnappyCompressor'};
```

Loading the dataset
===================

The data can be loaded using three different methods:

-   Alt 1: Using SSTables: through the *sstableloader* utility
-   Alt 2: Using the CSV file: copying the data from the file into the table (very similar to the COPY TO statement of SQL databases)
-   Alt 3: Using the JSON dump: through the *json2sstable* utility

Data loaded using the *json2sstable* method will not be available until the database is restarted. 
In contrast, data loaded with *sstableloader* or copied from CSV will be available immediately. Given 
that the *json2sstable* method is primarily intended for testing and debugging purposes, we do not recommend 
using it outside those scenarios. Nonetheless we include it in this tutorial for completion.

Alt 1: Using SSTables
---------------------

We will use sttableloader to load the Page table contents ([crawler-Page.tgz](http://docs.openstratio.org/resources/datasets/crawler-Page.tgz "Tar containing the dataset in SSTable format")). The listdomains one will remain empty for now, we will use it later to store results of operations computed on “Page”:

```shell-session
$ cd /PATH/TO/DATASET
$ tar -zxvf crawler-Page.tgz
$ sstableloader -d localhost crawler/Page/
```

You should get an output similar to the following:

```shell-session
Established connection to initial hosts
Opening sstables and calculating sections to stream
Streaming relevant part of crawler/Page/crawler-Page-jb-5-Data.db crawler/Page/crawler-Page-jb-6-Data.db to [/127.0.0.1]
progress: [/127.0.0.1 2/2 (100%)] [total: 100% - 14MB/s (avg: 18MB/s)]
```

Open a CQL shell to verify the data has been correctly loaded:

```shell-session
cqlsh> use crawler;
cqlsh:crawler> select count(*) from "Page" limit 30000;
```

There should be 21992 rows in the table.

Alt 2: Using the CSV file
-------------------------

-   Start the CQL shell.
-   Enter the following statements to load the content of the CSV file ([crawler-Page.csv](http://docs.openstratio.org/resources/datasets/crawler-Page.csv "Table "Page" in CSV format")) into the table:

```shell-session
cqlsh> use crawler;
cqlsh:crawler> copy "Page" (key, "___class", charset, content, domainName, downloadTime, enqueuedForTransforming, etag, firstDownloadTime, lastModified, responseCode, responseTime, timeTransformed, title, url)
  from '/PATH/TO/FILE/crawler-Page.csv'
  with header='true';
```

Once the process has completed, you should see a message saying 21992 rows have been imported. You can double-check using:

```shell-session
cqlsh:crawler> select count(*) from "Page" limit 30000;
```

Alt 3: Using the JSON dump
--------------------------

As stated at the beginning of this section, this method is not recommended other than for testing and 
debugging purposes. Use json2sstable to import the JSON data 
([crawler-Page.json](http://docs.openstratio.org/resources/datasets/crawler-Page.json "Table "Page" in JSON format")):

```shell-session
 $ cd /PATH/TO/FILE/
 $ json2sstable -K crawler -c Page crawler-Page.json /var/cassandra/data/crawler/Page/crawler-Page-jb-1-Data.db
```

That will produce the following output:

```shell-session
Importing 21992 keys...
Currently imported 1891 keys.
21992 keys imported successfully.
```

Start the CQL shell and check there are 21992 rows in the “Page” table:

```shell-session
cqlsh> use crawler;
cqlsh:crawler> select count(*) from "Page" limit 30000;
```

If not, then restart your Cassandra cluster (service cassandra restart). The data should become visible upon restart.

Using the Stratio Deep Shell
============================

The Stratio Deep shell provides a Scala interpreter that allows for interactive calculations on Cassandra RDDs. 
In this section, you are going to learn how to create RDDs of the Cassandra dataset we imported in the previous 
section and how to make basic operations on them. Start the shell:

```shell-session
$ stratio-deep-shell
```

A welcome screen will be displayed (figure 2).

![Stratio Deep shell Welcome Screen](images/t10-deepshell.png)

Figure 2: The Stratio Deep shell welcome screen

Step 1: Creating a RDD
----------------------

When using the Stratio Deep shell, a deepContext object has been created already and is available for use. 
The deepContext is created from the SparkContext and tells Stratio Deep how to access the cluster. However 
the RDD needs more information to access Cassandra data such as the keyspace and table names. By default, the 
RDD will try to connect to “localhost” on port “9160”, this can be overridden by setting the host and port 
properties of the configuration object: Define a configuration object for the RDD that contains the connection 
string for Cassandra, namely the keyspace and the table name:

```shell-session
scala> val config : ICassandraDeepJobConfig[Cells] = Cfg.create().host("localhost").rpcPort(9160).keyspace("crawler").table("Page").initialize
```

Create an RDD in the Deep context using the configuration object:

```shell-session
scala> val rdd: CassandraRDD[Cells] = deepContext.cassandraGenericRDD(config)
```

Step 2: Filtering data
----------------------

The CassandraRDD class provides a filter method that returns a new RDD containing only the elements 
that satisfy a predicate. We will use it to obtain a RDD with pages from domains containing the “abc.es” string:

```shell-session
scala> val containsAbcRDD = rdd filter {c :Cells => c.getCellByName("domainName").getCellValue.asInstanceOf[String].contains("abc.es") }
```

Count the number of rows in the resulting object:

```shell-session
scala> containsAbcRDD.count
```

Step 3: Caching data
--------------------

The RDD class, extended by CassandraRDD, provides a straightforward method for caching:

```shell-session
scala> val containsAbcCached = containsAbcRDD.cache
```

In turn, cached RDD can be filtered the same way it is done on non-cached RDDs. In this case, the content 
of the RDD is filtered on the “responseCode” column:

```shell-session
scala> val responseOkCached = containsAbcCached filter { c:Cells => c.getCellByName("responseCode").getCellValue == java.math.BigInteger.valueOf(200) }
```

Step 4: Grouping data
---------------------

A two steps method can be used to group data. Firstly the data is transformed into a list of key-value 
pairs and then grouped by key. Transformation into key-value pairs:

```shell-session
scala> val byDomainPairs = rdd map { c:Cells => (c.getCellByName("domainName").getCellValue.asInstanceOf[String], c) }
```

Grouping by domain name:

```shell-session
scala> val domainsGroupedByKey = byDomainPairs.groupByKey
```

Count the number of pages for each domain:

```shell-session
scala> val numPagePerDomainPairs = domainsGroupedByKey map { t:(String, Iterable[Cells]) => ( t._1, t._2.size ) }
```

Step 5: Writing the results to Cassandra
----------------------------------------

From the previous step we have a RDD object “numPagePerDomainPairs” that contains pairs of domain name 
(String) and the number of pages for that domain (Integer). To write this result to the listdomains table, 
we will need a configuration that binds the RDD to the given table and then write its content to Cassandra 
using that configuration. The first step is to get valid objects to write to Cassandra: cells. Cassandra 
cells for populating the “listdomains” table are obtained by applying a transformation function to the 
tuples of the CassandraRDD object “numPagePerDomainPairs” to construct the cells:

```shell-session
scala> val outRDD = numPagePerDomainPairs map { t: (String, Int) => 
    val domainNameCell = Cell.create("domain", t._1, true, false);
    val numPagesCell = Cell.create("num_pages", t._2);
    new Cells(domainNameCell, numPagesCell) 
}
```

Now that we have a RDD of cells to be written, we create the new configuration for the listdomains table:

```shell-session
scala> val outConfig = Cfg.createWriteConfig().host("localhost").rpcPort(9160).keyspace("crawler").table("listdomains").initialize
```

Then write the outRDD to Cassandra:

```shell-session
scala> com.stratio.deep.rdd.CassandraRDD.saveRDDToCassandra(outRDD, outConfig)
```

To check that the data has been correctly written to Cassandra, exit the Deep shell, open a CQL shell and 
look at the contents of the “listdomains” table:

```shell-session
$ cqlsh
cqlsh> use crawler;
cqlsh:crawler> select * from listdomains;
```

Where to go from here
=====================

Congratulations! You have completed the “First steps with Stratio Deep” tutorial. If you want to 
learn more, we recommend the "[Creating an Entity Object for Stratio Deep and Cassandra](t30-entity-object-cassandra.html "Creating an Entity Object for Stratio Deep and Cassandra")" tutorial.

Troubleshooting
===============

In this section we describe the most common problems that can be encountered when following this tutorial. For 
errors not reported here, please refer to the documentation of the issuing component.

“TSocket read 0 bytes” when bulk loading data into Cassandra
------------------------------------------------------------

This error may occur when copying a CSV file into a table:

```shell-session
cqlsh:crawler> copy "Page" (...) from 'crawler-Page.csv' with header='true';

TSocket read 0 bytes
```

It usually kills the Cassandra process. The most likely reason for this error is having insufficient memory 
for the heap. Try to increase the maximum heap size in your cassandra-env.sh file by uncommenting the following lines:

```bash
MAX_HEAP_SIZE="4G"
HEAP_NEWSIZE="800M"
```

Those are the default values provided in the configuration file and they should work for a machine with 
more than 4Gb of memory. Depending on your machine, you may try different values. Do not forget to stop 
and restart your Cassandra service after changing those parameters.

```shell-session
$ /PATH/TO/CASSANDRA/bin/cassandra -f
```

NullPointer exception when writing to Cassandra
-----------------------------------------------

The error looks like the following:

```shell-session
ERROR [Executor task launch worker-2] Executor:86 - Exception in task ID xxxx
java.lang.NullPointerException at org.apache.cassandra.dht.Murmur3Partitioner.getToken(Murmur3Partitioner.java:89)
...
```

The most likely reason is a missing PRIMARY KEY in the destination table or that the corresponding cell has 
not been defined as a partition key from Stratio Deep. Make sure the partition key parameter has been set to 
true when defining the cell corresponding to, or part of, the PRIMARY KEY:

```shell-session
val domainNameCell = Cell.create("domain", t._1, true, false);
```
