Stratio Deep
=====================

[![Build Status](https://drone.io/github.com/Stratio/stratio-deep/status.png)](https://drone.io/github.com/Stratio/stratio-deep/latest)

Stratio Deep is a thin integration layer between Apache Spark and Apache Cassandra. The integration is _not_ based on the Cassandra's Hadoop interface.
Stratio Deep is one of the core modules on which [Stratio's BigData platform (SDS)](http://www.stratio.com/) is based.

StratioDeep comes with an user friendly API that lets developers create Spark RDDs mapped to Cassandra column families.
We provide two different interfaces:

  * The first one will let developers map Cassandra tables to object entities (POJOs), just like if you were using any other ORM.
    This abstraction is quite handy, it will let you work on RDD<YourEntityHere> (under the hood StratioDeep will map columns to entity properties).
    Your domain entities must be correctly annotated using StratioDeep annotations (see StratioDeepExample example entities in package com.stratio.deep.testentity).

  * The second one is a more generic 'cell' API, that will let devs work on RDD<com.stratio.deep.entity.Cells> where a Cells object is a collection of com.stratio.deep.entity.Cell object.
    Column metadata is automatically fetched from the data store. This interface is a little bit more cumbersome to work with (see the example below),
    but has the advantage that it doesn't require the definition of additional entity classes.
    Example: you have a table called 'users' and you decide to use the 'Cells' interface. Once you get an instance 'c' of the Cells object,
    to get the value of column 'address' you can issue a c.getCellByName("address").getCellValue().
    Please, refer to the Deep API documentation to know more about the Cells and Cell objects.

We encourage you to read the more comprehensive documentation hosted on the [Stratio website](http://wordpress.dev.strat.io/examples/).

Stratio Deep comes with an example sub project called StratioDeepExamples containing a set of working examples, both in Java and Scala.
Please, refer to StratioDeepExample README for further information on how to setup a working environment.

Requirements
============

  * Cassandra 2.0.5
  * Spark 0.9.1
  * Apache Maven >= 3.0.4
  * Java 1.7
  * Scala 2.10.3

Configure the development and test environment
==============================================
* Clone the project
* To configure a development environment in Eclipse: import as Maven project. In IntelliJ: open the project by selecting the deep-parent POM file
* Install the project in you local maven repository. Enter deep-parent subproject and perform: mvn clean install (add -DskipTests to skip tests)
* Put Stratio Deep to work on a working cassandra + spark cluster. You have several options:
    * Download a pre-configured Stratio platform VM [Stratio's BigData platform (SDS)](http://www.stratio.com/).
      This VM will work on both Virtualbox and VMWare, and comes with a fully configured distribution that also includes Stratio Deep. We also distribute the VM with several preloaded datasets in Cassandra. This distribution will include Stratio's customized Cassandra distribution containing our powerful [open-source lucene-based secondary indexes](https://github.com/Stratio/stratio-cassandra), see Stratio documentation for further information.
      Once your VM is up and running you can test Deep using the shell. Enter /opt/sds and run bin/stratio-deep-shell.
    * Install a new Stratio cluster using the Stratio installer. Please refer to Stratio's website to download the installer and its documentation.
    * You already have a working Cassandra server on your development machine: you need a spark+deep bundle, we suggest to create one by running:
    
	    ``cd deep-parent``
	    
	    ``./make-distribution-deep.sh``
	    
    this will build a Spark distribution package with StratioDeep and Cassandra's jars included (depending on your machine this script could take a while, since it will compile Spark from sources).
The package will be called ``spark-deep-distribution-X.Y.Z.tgz``, untar it to a folder of your choice, enter that folder and issue a ``./stratio-deep-shell``, this will start an interactive shell where you can test StratioDeep (you may have noticed this is will start a development cluster started with MASTER="local").

    * You already have a working installation os Cassandra and Spark on your development machine: this is the most difficult way to start testing StratioDeep, but you know what you're doing  you will have to
        1. copy the Stratio Deep jars to Spark's 'jars' folder (``$SPARK_HOME/jars``).
        2. copy Cassandra's jars to Spark's 'jar' folder.
        3. copy Datastax Java Driver jar (v 2.0.x) to Spark's 'jar' folder.
        4. start spark shell and import the following:
        
			``import com.stratio.deep.config._``			
			``import com.stratio.deep.entity._``			
			``import com.stratio.deep.context._``			
			``import com.stratideep.rdd._``
			

First steps
===========
Once you have a working development environment you can finally start testing StratioDeep. This are the basic steps you will always have to perform in order to use StratioDeep:

* __Build an instance of a configuration object__: this will let you tell StratioDeep the Cassandra endpoint, the Cassandra keyspace and table you want to access and much more.
  It will also let you specify which interface to use (the domain entity or the generic interface).
  We have a factory that will help you create a configuration object using a fluent API. Creating a configuration object is an expensive operation.
  Please take the time to read the java and scala examples provided in StratioDeepExamples subproject and to read the comprehensive Stratio Deep documentation at [Stratio website](http://wordpress.dev.strat.io/examples/).
* __Create an RDD__: using the DeepSparkContext helper methods and providing the configuration object you've just instantiated.
* __Perform some computation over this RDD(s)__: this is up to you, we only help you fetching the data efficiently from Cassandra, you can use the powerful [Spark API](https://spark.apache.org/docs/0.9.0/api/core/index.html#org.apache.spark.package).
* __(optional) write the computation results out to Cassandra__: we provide a way to efficiently save the result of your computation to Cassandra.
  In order to do that you must have another configuration object where you specify the output keyspace/column family. We can create the output column family for you if needed.
  Please, refer to the comprehensive Stratio Deep documentation at [Stratio website](http://wordpress.dev.strat.io/examples/).