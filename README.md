Stratio Deep
=====================

Stratio Deep is a thin integration layer between Apache Spark and Apache Cassandra. The integration is currently
based on the Cassandra's Hadoop interface.
Stratio Deep is one of the core modules on which [Stratio's BigData platform (SDS)](http://www.stratio.com/) is based.

StratioDeep comes with an user friendly API that lets developers:

  * Map their Cassandra's column families to object entities, and use this handy abstraction to perform
  subsequent calculations using this high-level abstraction.
  * Perform computation using a generic 'cell' API, when the definition of an entity object is not needed or not feasible.

We encourage you to read the comprehensive documentation hosted on the [Stratio website](http://wordpress.dev.strat.io/examples/).

StratioDeep comes with an example sub project called StratioDeepExamples containing a set of working examples on how to use StratioDeep, both from Java and Scala.
Please, refer to StratioDeepExample README for further information on how to setup a working environment.

Requirements TODO
=================

  * Cassandra 2.0.5
  * Stratio Deep 0.1.0
  * Apache Maven >= 3.0.4
  * Java7


How to start
============

  * Clone the project

  * Eclipse: import as Maven project. IntelliJ: open the project by selecting the StratioDeepParent POM file

  * To install the project (with tests), enter StratioDeepParent and perform:
        mvn clean install

  Take into account for a couple of minutes in order to tests to pass, integration tests start both an embedded Cassandra and Spark server.
