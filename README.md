What is Deep?
=====================

Deep is a thin integration layer between Apache Spark and several NoSQL datastores.
We actually support Apache Cassandra and MongoDB, but in the near future we will add support for sever other datastores.

Apache Cassandra integration
============================

The integration is _not_ based on the Cassandra's Hadoop interface.

Deep comes with an user friendly API that lets developers create Spark RDDs mapped to Cassandra column families.
We provide two different interfaces:

  * The first one will let developers map Cassandra tables to plain old java objects (POJOs), just like if you were using any other ORM. We call this API the 'entity objects' API.
    This abstraction is quite handy, it will let you work on RDD<YourEntityHere> (under the hood Deep will transparently map Cassandra's columns to entity properties).
    Your domain entities must be correctly annotated using Deep annotations (take a look at deep-examples example entities in package com.stratio.deep.testentity).

  * The second one is a more generic 'cell' API, that will let developerss work on RDD<com.stratio.deep.entity.Cells> where a 'Cells' object is a collection of com.stratio.deep.entity.Cell objects.
    Column metadata is automatically fetched from the data store. This interface is a little bit more cumbersome to work with (see the example below),
    but has the advantage that it doesn't require the definition of additional entity classes.
    Example: you have a table called 'users' and you decide to use the 'Cells' interface. Once you get an instance 'c' of the Cells object,
    to get the value of column 'address' you can issue a c.getCellByName("address").getCellValue().
    Please, refer to the Deep API documentation to know more about the Cells and Cell objects.

We encourage you to read the more comprehensive documentation hosted on the [Openstratio website](http://www.openstratio.org/examples/using-stratio-deep/).

Deep comes with an example sub project called 'deep-examples' containing a set of working examples, both in Java and Scala.
Please, refer to the deep-example project README for further information on how to setup a working environment.

MongoDB integration
===================

Spark-MongoDB connector is based on Hadoop-mongoDB.

Support for MongoDB has been added in version 0.3.0 and is not yet considered a complete feature.

We provide two different interfaces:

  * ORM API, you just have to annotate your POJOs with Deep annotations and magic will begin, you will be able to connect MongoDB with Spark using your own model entities.

  * Generic cell API, you do not need to specify the collection's schema or add anything to your POJOs, each document will be transform to an object "Cells".

We added a few working examples for MongoDB in deep-examples subproject, take a look at:

Entities:

  * com.stratio.deep.examples.java.ReadingEntityFromMongoDB
  * com.stratio.deep.examples.java.WritingEntityToMongoDB
  * com.stratio.deep.examples.java.GroupingEntityWithMongoDB

Cells:

  * com.stratio.deep.examples.java.ReadingCellFromMongoDB
  * com.stratio.deep.examples.java.WritingCellToMongoDB
  * com.stratio.deep.examples.java.GroupingCellWithMongoDB


You can check out our first steps guide here:

http://www.openstratio.org/tutorials/first-steps-with-stratio-deep-and-mongodb/


We are working on further improvements!

Requirements
============

  * Cassandra, we tested versions from 1.2.8 up to 2.0.8 (for Spark <=> Cassandra integration).
  * MongoDB, we tested the integration with MongoDB versions 2.2, 2.4 y 2.6 using Standalone, Replica Set and Sharded Cluster (for Spark <=> MongoDB integration).
  * Spark 1.0.0
  * Apache Maven >= 3.0.4
  * Java 1.7
  * Scala 2.10.3

Configure the development and test environment
==============================================
* Clone the project
* To configure a development environment in Eclipse: import as Maven project. In IntelliJ: open the project by selecting the deep-parent POM file
* Install the project in you local maven repository. Enter deep-parent subproject and perform: mvn clean install (add -DskipTests to skip tests)
* Put Deep to work on a working cassandra + spark cluster. You have several options:
    * Download a pre-configured Stratio platform VM [Stratio's BigData platform (SDS)](http://www.stratio.com/).
      This VM will work on both Virtualbox and VMWare, and comes with a fully configured distribution that also includes Stratio Deep. We also distribute the VM with several preloaded datasets in Cassandra. This distribution will include Stratio's customized Cassandra distribution containing our powerful [open-source lucene-based secondary indexes](https://github.com/Stratio/stratio-cassandra), see Stratio documentation for further information.
      Once your VM is up and running you can test Deep using the shell. Enter /opt/sds and run bin/stratio-deep-shell.
    * Install a new cluster using the Stratio installer. Please refer to Stratio's website to download the installer and its documentation.
    * You already have a working Cassandra server on your development machine: you need a spark+deep bundle, we suggest to create one by running:
    
	    ``cd deep-scripts``
	    
	    ``./make-distribution-deep.sh``
	    
    this will build a Spark distribution package with StratioDeep and Cassandra's jars included (depending on your machine this script could take a while, since it will compile Spark from sources).
The package will be called ``spark-deep-distribution-X.Y.Z.tgz``, untar it to a folder of your choice, enter that folder and issue a ``./stratio-deep-shell``, this will start an interactive shell where you can test StratioDeep (you may have noticed this is will start a development cluster started with MASTER="local").

    * You already have a working installation os Cassandra and Spark on your development machine: this is the most difficult way to start testing Deep, but you know what you're doing you will have to
        1. copy the Stratio Deep jars to Spark's 'jars' folder (``$SPARK_HOME/jars``).
        2. copy Cassandra's jars to Spark's 'jar' folder.
        3. copy Datastax Java Driver jar (v 2.0.x) to Spark's 'jar' folder.
        4. start spark shell and import the following:
        
			``import com.stratio.deep.config._``			
			``import com.stratio.deep.entity._``			
			``import com.stratio.deep.context._``			
			``import com.stratideep.rdd._``
			

Once you have a working development environment you can finally start testing Deep. This are the basic steps you will always have to perform in order to use Deep:

First steps with Spark and Cassandra
====================================

* __Build an instance of a configuration object__: this will let you tell Deep the Cassandra endpoint, the keyspace, the table you want to access and much more.
  It will also let you specify which interface to use (the domain entity or the generic interface).
  We have a factory that will help you create a configuration object using a fluent API. Creating a configuration object is an expensive operation.
  Please take the time to read the java and scala examples provided in 'deep-examples' subproject and to read the comprehensive documentation at [OpenStratio website](http://www.openstratio.org/examples/using-stratio-deep/).
* __Create an RDD__: using the DeepSparkContext helper methods and providing the configuration object you've just instantiated.
* __Perform some computation over this RDD(s)__: this is up to you, we only help you fetching the data efficiently from Cassandra, you can use the powerful [Spark API](https://spark.apache.org/docs/1.0.0/api/java/index.html).
* __(optional) write the computation results out to Cassandra__: we provide a way to efficiently save the result of your computation to Cassandra.
  In order to do that you must have another configuration object where you specify the output keyspace/column family. We can create the output column family for you if needed.
  Please, refer to the comprehensive Stratio Deep documentation at [Stratio website](http://www.openstratio.org/examples/using-stratio-deep/).

First steps with Spark and MongoDB
==================================

* __Build an instance of a configuration object__: this will let you tell Stratio Deep the MongoDB endpoint, the MongoDB database and collection you want to access and much more.
  It will also let you specify which interface to use (the domain entity).
  We have a factory that will help you create a configuration object using a fluent API. Creating a configuration object is an expensive operation.
  Please take the time to read the java and scala examples provided in 'deep-examples' subproject and to read the comprehensive Deep documentation at [OpenStratio website](http://www.openstratio.org/examples/using-stratio-deep/).
* __Create an RDD__: using the DeepSparkContext helper methods and providing the configuration object you've just instantiated.
* __Perform some computation over this RDD(s)__: this is up to you, we only help you fetching the data efficiently from MongoDB, you can use the powerful [Spark API](https://spark.apache.org/docs/1.0.0/api/java/index.html).
* __(optional) write the computation results out to MongoDB__: we provide a way to efficiently save the result of your computation to MongoDB.

Migrating from version 0.2.9
============================
From version 0.4.x, Deep supports multiple datastores, in order to correctly implement this new feature Deep has undergone an huge refactor between versions 0.2.9 and 0.4.x. To port your code to the new version you should take into account a few changes we made.

New Project Structure
---------------------
From version 0.4.x, Deep supports multiple datastores, in your project you should import only the maven dependency you will use: deep-cassandra or deep-mongodb.

Changes to 'com.stratio.deep.entity.Cells'
------------------------------------------

* Until version 0.4.x the 'Cells' was implicitly associated to a record coming from a specific table. When performing a join in Spark, 'Cell' objects coming from different tables are mixed into an single 'Cells' object.
  Deep now keeps track of the original table a Cell object comes from, changing the internal structure of 'Cells', where each 'Cell' is associated to its 'table'.
    1. If you are a user of 'Cells' objects returned from Deep, nothing changes for you. The 'Cells' API keeps working as usual.
    2. If you manually create 'Cells' objects you can keep using the original API, in this case each Cell you add to your Cells object is automatically associated to a default table name.
    3. You can specify the default table name, or let Deep chose an internal default table name for you.
    4. We added a new constructor to 'Cells' accepting the default table name. This way the 'old' API will always manipulate 'Cell' objects associated to the specified default table.
    5. For each method manipulating the content of a 'Cells' object, we added a new method that also accepts the table name: if you call the method	 whose signature does _not_ have the table name, the table action is performed over the Cell associated to the default table, otherwise the action is performed over the 'Cell'(s) associated to the specified table. 
    6. size() y isEmpty() will compute their results taking into account all the 'Cell' objects contained. 
    7. size(String tableName) and isEmpty(tableName) compute their result taking into account only the 'Cell' objects associated to the specified table.  
    8. Obviously, when dealing with Cells objects, Deep always associates a Cell to the correct table name.
    
Examples:
<pre>
Cells cells1 = new Cells(); // instantiate a Cells object whose default table name is generated internally.
Cells cells2 = new Cells("my_default_table"); // creates a new Cells object whose default table name is specified by the user
cells2.add(new Cell(...)); // adds to the 'cells2' object a new Cell object associated to the default table
cells2.add("my_other_table", new Cell(...)); // adds to the 'cells2' object a new Cell associated to "my_other_table"  
</pre>

Changes to objects hierarchy
-----------------------------------------------------------
* IDeepJobConfig interface has been splitted into ICassandraDeepJobConfig and IMongoDeepJobConfig sub-interfaces. Each sub-interface exposes only the configuration properties that make sense for each data base.
com.stratio.deep.config.DeepJobConfigFactory's factory methods now return the proper subinterface.
* __DeepSparkContext__ has been splitted into __CassandraDeepSparkContext__ and __MongoDeepSparkContext__.
* __DeepJobConfigFactory__ has been renamed to __ConfigFactory__ (to reduce verbosity).

RDD creation
----------------
Methods used to create Cell and Entity RDD has been merged into one single method:

* __CassandraDeepSparkContext__: cassandraEntityRDD(...) and cassandraGenericRDD(...) has been merged to cassandraRDD(...)
* __MongoDeepSparkContext__: mongoEntityRDD(...) and mongoCellRDD(...) has been merged to mongoRDD(...)
