First Steps with Stratio Deep and databases accessible through JDBC
==================================================================

StratioDeep-JDBC is an integration layer between Spark, a distributed computing framework and JDBC, 
a Java database accesibility technology. [JDBC](http://www.oracle.com/technetwork/java/javase/jdbc/index.html "JDBC website") 
provides an API that defines how a client may access a Database. [Spark](http://spark.incubator.apache.org/ "Spark website") 
is a fast and general-purpose cluster computing system that can run applications up to 100 times faster than Hadoop. 
It processes data using Resilient Distributed Datasets (RDDs) allowing storage of intermediate results in memory 
for future processing reuse. Spark applications are written in 
[Scala](http://www.scala-lang.org/ "The Scala programming language site"), a popular functional language for 
the Java Virtual Machine (JVM). Integrating JDBC accessible database and Spark gives us a system that combines the best of both 
worlds opening to those databases the possibility of solving a wide range of new use cases.

Summary
=======

This tutorial shows how Stratio Deep can be used to perform simple to complex queries and calculations on data 
stored in a database accesible through JDBC. You will learn:

-   How to use the Stratio Deep interactive shell.
-   How to create a RDD from a JDBC connection and perform operations on the data.
-   How to write data from a RDD to a database using a JDBC connection.

Table of Contents
=================

-   [Summary](#summary)
-   [Before you start](#before-you-start)
    -   [Prerequisites](#prerequisites)
-   [Loading the dataset](#loading-the-dataset)
-   [Using the Stratio Deep Shell](#using-the-stratio-deep-shell)
    -   [Step 1: Creating a RDD](#step-1-creating-a-rdd)
    -   [Step 2: Word Count](#step-2-word-count)
    -   [Step 3: Writing the results to MySQL](#step-3-writing-the-results-to-mysql)
-   [Where to go from here](#where-to-go-from-here)

Before you start
================

Prerequisites
-------------

-   MySQL and Stratio Deep: see [Getting Started](/getting-started.html "Getting Started") for installation instructions
-   Basic knowledge of SQL, Java and/or Scala
-	Some input data loaded into MySQL.

Loading the dataset
-------------------

First of all, you need to create the MySQL database and tables used in the example. We will use a "test" database. Open the
MySQL shell:

```shell-session
mysql -u [your user] -p[your password]
}
```

Now execute the SQL command that will create the database:

```shell-session
mysql> CREATE DATABASE test;
Query OK, 1 row affected (0.01 sec)
```

Create the tables that will be used in this tutorial:

```shell-session
mysql> use test;
mysql> CREATE TABLE `test_table` (`id` varchar(256) NOT NULL DEFAULT '', `message` varchar(256) DEFAULT NULL, `number` bigint(20) DEFAULT NULL, PRIMARY KEY (`id`)) ENGINE=InnoDB;
Query OK, 1 row affected (0.01 sec)

mysql> CREATE TABLE `test_table_output` (`word` varchar(256) NOT NULL DEFAULT '', `count` bigint(20) DEFAULT NULL, PRIMARY KEY (`word`)) ENGINE=InnoDB;
Query OK, 1 row affected (0.01 sec)
```

Now, insert some test data into the input table:

```shell-session
mysql> INSERT INTO `test_table` (`id`, `message`, `number`) VALUES ('1', 'test message', 1);
Query OK, 1 row affected (0.01 sec)
mysql> INSERT INTO `test_table` (`id`, `message`, `number`) VALUES ('2', 'message 2', 1);
Query OK, 1 row affected (0.01 sec)
```

From the same MySQL shell, check that there are 2 rows in the “test_table” table:

```shell-session
mysql> select * from test_table;
```

Using the Stratio Deep Shell
============================

The Stratio Deep shell provides a Scala interpreter that allows interactive calculations on JDBC RDDs. In 
this section, you are going to learn how to create RDDs of the database dataset we imported in the previous 
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
the RDD needs more information to access MySQL data such as the schema and table names. Define a configuration object for the RDD that contains the connection string for MySQL, namely the database and the table name:

```shell-session
scala> val inputConfigCell: JdbcDeepJobConfig[Cells] = JdbcConfigFactory.createJdbc.host(host).port(port).username(user).password(password).driverClass(driverClass).database(database).table(table)
scala> inputConfigCell.initialize
```

Create a RDD in the Deep context using the configuration object:

```shell-session
scala> val carPrices: RDD[Cells] = deepContext.createRDD(inputConfigCell)
```

Step 2: Word Count
------------------

We create a JavaRDD&lt;String> from the MessageTestEntity

```shell-session
scala> val words: RDD[String] = inputRDDEntity flatMap {
      e: Cells => (for (message <- e.getCellByName("message")) yield message.split(" ")).flatten
    }
```

Now we make a JavaPairRDD&lt;String, Integer>, counting one unit for each word

```shell-session
scala> val wordCount : RDD[(String, Long)] = words map { s:String => (s,1) }
```

We group by word

```shell-session
scala> val wordCountReduced  = wordCount reduceByKey { (a,b) => a + b }
```

Create a new WordCount Object from

```shell-session
scala> val outputRDD = wordCountReduced map { e:(String, Long) => new WordCount(e._1, e._2) }
```

Step 3: Writing the results to MySQL
------------------------------------

From the previous step we have a RDD object “outputRDD” that contains pairs of word (String)
and the number of occurrences (Integer). To write this result to the output collection, we will need
a configuration that binds the RDD to the given collection and then writes its contents to MySQL 
using that configuration:

```shell-session
scala> val outputConfigEntity: JdbcDeepJobConfig[WordCount] = JdbcConfigFactory.createJdbc(classOf[WordCount]).host(host).port(port).username(user).password(password).driverClass(driverClass).database(database).table(table)
```

Then write the outRDD to MySQL:

```shell-session
scala>DeepSparkContext.saveRDD(outputRDD, outputConfigEntity)
```

To check that the data has been correctly written to MySQL, open a MySQL shell and look at the contents 
of the “output” collection:

```shell-session
$ mysql -u [your user] -p[your password]
mysql> use test;
mysql> select * from test_table_output;
```

Where to go from here
=====================

Congratulations! You have completed the “First steps with Stratio Deep” tutorial. If you want to learn more, 
we recommend the “[Writing and Running a Basic Application](t40-basic-application.html "Writing and Running a Basic Application")” tutorial.
