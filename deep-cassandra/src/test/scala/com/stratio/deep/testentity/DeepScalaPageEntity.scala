/*
 * Copyright 2014, Stratio.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.stratio.deep.testentity

import com.stratio.deep.commons.annotations.{DeepEntity, DeepField}
import com.stratio.deep.commons.entity.IDeepType
import org.apache.cassandra.db.marshal.{Int32Type, LongType}

import scala.beans.BeanProperty

@DeepEntity class DeepScalaPageEntity extends IDeepType {

  @BeanProperty
  @DeepField(isPartOfPartitionKey = true)
  var id: String = null

  @BeanProperty
  @DeepField(fieldName = "domain_name")
  var domain: String = null

  @BeanProperty
  @DeepField
  var url: String = null

  @BeanProperty
  @DeepField(validationClass = classOf[Int32Type], fieldName = "response_time")
  var responseTime: Integer = _

  @BeanProperty
  @DeepField(fieldName = "response_code", validationClass = classOf[Int32Type])
  var responseCode: Integer = _

  @BeanProperty
  @DeepField(validationClass = classOf[LongType], fieldName = "download_time")
  var downloadTime: java.lang.Long = _
}
