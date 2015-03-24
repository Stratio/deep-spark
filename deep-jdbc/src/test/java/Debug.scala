/**
 * Created by jmgomez on 18/03/15.
 */
import com.stratio.deep.core.context.{DeepSparkConfig, DeepSparkContext}
import com.stratio.deep.jdbc.config._
import com.stratio.deep.core.rdd._

object Debug extends App with Constants{

  //Configure DeepSparkConfig
  val deepSpakConfig = new DeepSparkConfig();
  deepSpakConfig.setMaster(MASTER)
  deepSpakConfig.setAppName("DEBUG")
  deepSpakConfig.setSparkHome(SPARK_HOME)
  deepSpakConfig.setJars(SPARK_JARS)

  //Create the JDBC connection config
  val configOracle = JdbcConfigFactory.createJdbc().connectionUrl(CONNECTOR_URL).host(HOST).port(PORT).username(USER_NAME).password(PASSWORD).driverClass(DRIVER_CLASS).database(DATABASE).catalog(CATALOG).table(TABLE)

  val deepContext = new DeepSparkContext(deepSpakConfig)

  val rdd = deepContext.createRDD(configOracle)

  System.out.println(rdd.count())
}

sealed trait Constants {

  val MASTER = "spark://jmgomez-HP-ENVY-15-Notebook-PC:7077"
  val SPARK_HOME = "/var/sds/stratio/spark"
  val SPARK_JARS = Array[String]()
  val CONNECTOR_URL = "jdbc:oracle:thin:@10.200.0.162:1521:NEBULA"
  val HOST = "10.200.0.162"
  val PORT = 1521
  val USER_NAME ="STRATIO"
  val PASSWORD = "stratio"
  val DRIVER_CLASS = "oracle.jdbc.driver.OracleDriver"
  //val DRIVER_CLASS = "com.mysql.jdbc.Driver"
  val DATABASE = "STRATIO"
  val CATALOG = "STRATIO"
  val TABLE = "USE_DEMO"

}
