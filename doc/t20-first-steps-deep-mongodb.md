---
title: First Steps with Stratio Deep and MongoDB
---

StratioDeep-MongoDB is an integration layer between Spark, a distributed computing framework and MongoDB, 
a NoSQL database. [MongoDB](http://www.mongodb.org/ "MongoDB website") provides a documental data model 
richer than typical key/value systems. [Spark](http://spark.incubator.apache.org/ "Spark website") is a 
fast and general-purpose cluster computing system that can run applications up to 100 times faster than Hadoop. 
It processes data using Resilient Distributed Datasets (RDDs) allowing storage of intermediate results in memory 
for future processing reuse. Spark applications are written in 
[Scala](http://www.scala-lang.org/ "The Scala programming language site"), a popular functional language for 
the Java Virtual Machine (JVM). Integrating MongoDB and Spark gives us a system that combines the best of both 
worlds opening to MongoDB the possibility of solving a wide range of new use cases.

Summary
=======

This tutorial shows how Stratio Deep can be used to perform simple to complex queries and calculations on data 
stored in MongoDB. You will learn:

-   How to use the Stratio Deep interactive shell.
-   How to create a RDD from MongoDB and perform operations on the data.
-   How to write data from a RDD to MongoDB.

Table of Contents
=================

-   [Summary](#summary)
-   [Before you start](#before-you-start)
    -   [Prerequisites](#prerequisites)
    -   [Resources used in this tutorial](#resources-used-in-this-tutorial)
-   [Loading the dataset](#loading-the-dataset)
-   [Using the Stratio Deep Shell](#using-the-stratio-deep-shell)
    -   [Step 1: Creating a RDD](#step-1-creating-a-rdd)
    -   [Step 2: Word Count](#step-2-word-count)
    -   [Step 3: Writing the results to MongoDB](#step-3-writing-the-results-to-mongodb)
-   [Where to go from here](#where-to-go-from-here)

Before you start
================

Prerequisites
-------------

-   MongoDB and Stratio Deep: see [Getting Started](/getting-started.html "Getting Started") for installation instructions
-   Basic knowledge of SQL, Java and/or Scala

Resources used in this tutorial
-------------------------------

Dataset in JSON format: [divineComedy.json](http://docs.openstratio.org/resources/datasets/divineComedy.json)

Loading the dataset
-------------------

The data can be loaded using mongoimport command.

```shell-session
$ mongoimport -d book -c input divineComedy.json --jsonArray
```

That will produce the following output:

```shell-session
connected to: 127.0.0.1
imported 1 objects
```

Start the Mongo shell and check that there is 1 row in the “input” table:

```shell-session
> use book;
> db.input.count();
> db.input.findOne();
```

Using the Stratio Deep Shell
============================

The Stratio Deep shell provides a Scala interpreter that allows interactive calculations on MongoDB RDDs. In 
this section, you are going to learn how to create RDDs of the MongoDB dataset we imported in the previous 
section and how to make basic operations on them. Start the shell:

```shell-session
$ stratio-deep-shell
```

A welcome screen will be displayed (figure 1).

![Stratio Deep shell Welcome Screen](http://www.openstratio.org/wp-content/uploads/2014/01/stratio-deep-shell-WelcomeScreen.png)  
Figure 1: The Stratio Deep shell welcome screen

Step 1: Creating a RDD
----------------------

When using the Stratio Deep shell, a deepContext object has been created already and is available for use.
The deepContext is created from the SparkContext and tells Stratio Deep how to access the cluster. However
the RDD needs more information to access MongoDB data such as the database and collection names. By default,
the RDD will try to connect to “localhost” on port “27017”, this can be overridden by setting the host and
port properties of the configuration object: Define a configuration object for the RDD that contains the
connection string for MongoDB, namely the database and the collection name:

```shell-session
scala> import org.apache.spark.rdd._
scala> import scala.collection.JavaConversions._
scala> val inputConfigEntity: IMongoDeepJobConfig[BookEntity] = Cfg.createMongoDB(classOf[BookEntity]).host("localhost:27017").database("book").collection("input").initialize
```

Create a RDD in the Deep context using the configuration object:

```shell-session
scala> val inputRDDEntity: MongoEntityRDD[BookEntity] = deepContext.mongoEntityRDD(inputConfigEntity)
```

Step 2: Word Count
------------------

We create a JavaRDD&lt;String> from the BookEntity

```shell-session
scala> val words: RDD[String] = inputRDDEntity flatMap {
      e: BookEntity => (for (canto <- e.getCantoEntities) yield canto.getText.split(" ")).flatten
    }
```

Now we make a JavaPairRDD&lt;String, Integer>, counting one unit for each word

```shell-session
scala> val wordCount : RDD[(String, Integer)] = words map { s:String => (s,1) }
```

We group by word

```shell-session
scala> val wordCountReduced  = wordCount reduceByKey { (a,b) => a + b }
```

Create a new WordCount Object from

```shell-session
scala> val outputRDD = wordCountReduced map { e:(String, Integer) => new WordCount(e._1, e._2) }
```

Step 3: Writing the results to MongoDB
--------------------------------------

From the previous step we have a RDD object “outputRDDEntity” that contains pairs of word (String)
and the number of occurrence (Integer). To write this result to the output collection, we will need
a configuration that binds the RDD to the given collection and then writes its contents to MongoDB 
using that configuration:

```shell-session
scala> val outputConfigEntity: IMongoDeepJobConfig[WordCount] = Cfg.createMongoDB(classOf[WordCount]).host("localhost:27017").database("book").collection("output").initialize
```

Then write the outRDD to MongoDB:

```shell-session
scala> MongoEntityRDD.saveEntity(outputRDD, outputConfigEntity)
```

To check that the data has been correctly written to MongoDB, open a Mongo shell and look at the contents 
of the “output” collection:

```shell-session
$ mongo --host 127.0.0.1 --port 27017 book
> db.output.find().sort({"count":-1}).pretty()
```

Where to go from here
=====================

Congratulations! You have completed the “First steps with Stratio Deep” tutorial. If you want to learn more, 
we recommend the “[Writing and Running a Basic Application](t40-basic-application.html "Writing and Running a Basic Application")” tutorial.
