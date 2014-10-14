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

import com.stratio.deep.commons.annotations.{DeepField, DeepEntity}
import com.stratio.deep.commons.entity.IDeepType
import org.apache.cassandra.db.marshal._
import scala.beans.BeanProperty

/**
 * Author: Luca Rosellini
 * Date..: 21-mar-2014
 */
@DeepEntity class ScalaPageEntity extends IDeepType {

  @BeanProperty
  @DeepField(isPartOfPartitionKey = true, fieldName = "key")
  var id: String = _

  @BeanProperty
  @DeepField(fieldName = "domainName")
  var domain: String = _

  @BeanProperty
  @DeepField
  var url: String = _

  @BeanProperty
  @DeepField(validationClass = classOf[LongType], fieldName = "responseTime")
  var responseTime: java.lang.Long = _

  @BeanProperty
  @DeepField(fieldName = "responseCode", validationClass = classOf[IntegerType])
  var responseCode: java.math.BigInteger = _

  @BeanProperty
  @DeepField(validationClass = classOf[LongType], fieldName = "downloadTime")
  var downloadTime: java.lang.Long = _


}
