package com.stratio.deep.entity

import com.stratio.deep.annotations.{DeepField, DeepEntity}
import scala.beans.BeanProperty
import org.apache.cassandra.db.marshal.{LongType, Int32Type}

@DeepEntity class ScalaPageEntity extends IDeepType {

  @BeanProperty @DeepField(isPartOfPartitionKey = true)
  var id:String = null

  @BeanProperty @DeepField(fieldName = "domain_name")
  var domain: String = null

  @BeanProperty @DeepField
  var url: String = null

  @BeanProperty @DeepField(validationClass = classOf[Int32Type], fieldName = "response_time")
  var responseTime: Integer = _

  @BeanProperty @DeepField(fieldName = "response_code", validationClass = classOf[Int32Type])
  var responseCode: Integer = _

  @BeanProperty @DeepField(validationClass = classOf[LongType], fieldName = "download_time")
  var downloadTime: java.lang.Long = _
}
