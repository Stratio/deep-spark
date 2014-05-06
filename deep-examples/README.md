Stratio Deep Examples
=====================

This project contains java and scala examples for Stratio Deep. These examples are 
available also online at [StratioDoc website](http://wordpress.dev.strat.io/examples/) where references to
specific datamodels have been rewritten in a generic form.


Requirements
============

  * Cassandra 2.0.5
  * Stratio Deep 0.1.0
  * Apache Maven >= 3.0.4
  * Java7


How to start
============

  * Clone the project

  * Eclipse: import as Maven project. IntelliJ: open the project by selecting the POM file

  * To compile the project:
        mvn compile

  * To package the project:
        mvn package


Deep context configuration
==========================

Edit com.stratio.deep.utils.ContextProperties Java class and set
the attributes for your environment.


Datasets used in the examples
=============================

Datasets can be found in the [StratioDoc download area](http://docs.dev.strat.io/doc/tutorials/datasets/)
and instructions for importing them at:

  * Tweets dataset: [Creating a POJO tutorial](http://wordpress.dev.strat.io/devguides/tutorials/creating-a-pojo-for-stratio-deep/#creatingDataModel)
  * Crawler dataset: [First steps with Stratio Deep tutorial](http://wordpress.dev.strat.io/devguides/tutorials/first-steps-with-stratio-deep/#__RefHeading__2448_21369393)


Running an example
==================

  java -cp $CLASSPATH ExampleClassOrObject

Where CLASSPATH should contain all the jars from DEEP_HOME/jars/ and the StratioDeepExamples one.


List of Examples
================

Java:

  * com.stratio.deep.examples.java.AggregatingData
  * com.stratio.deep.examples.java.GroupingByColumn
  * com.stratio.deep.examples.java.MapReduceJob
  * com.stratio.deep.examples.java.WritingEntityToCassandra
  * com.stratio.deep.examples.java.CreatingCellRDD
  * com.stratio.deep.examples.java.GroupingByKey
  * com.stratio.deep.examples.java.WritingCellToCassandra

Scala:

  * com.stratio.deep.examples.scala.AggregatingData
  * com.stratio.deep.examples.scala.GroupingByColumn
  * com.stratio.deep.examples.scala.MapReduceJob
  * com.stratio.deep.examples.scala.WritingEntityToCassandra
  * com.stratio.deep.examples.scala.CreatingCellRDD
  * com.stratio.deep.examples.scala.GroupingByKey
  * com.stratio.deep.examples.scala.WritingCellToCassandra
  * com.stratio.deep.examples.scala.UsingScalaEntity
  * com.stratio.deep.examples.scala.UsingScalaCollectionEntity
