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

import java.util.UUID

import com.stratio.deep.commons.annotations.{DeepField, DeepEntity}
import com.stratio.deep.commons.entity.IDeepType
import org.apache.cassandra.db.marshal.{MapType, ListType, SetType}

import scala.beans.BeanProperty


/**
 * Created by luca on 27/03/14.
 */
@DeepEntity class ScalaCollectionEntity extends IDeepType {
  @BeanProperty
  @DeepField(isPartOfPartitionKey = true)
  var id: Integer = _

  @BeanProperty
  @DeepField(fieldName = "first_name")
  var firstName: String = _

  @BeanProperty
  @DeepField(fieldName = "last_name")
  var lastName: String = _

  @BeanProperty
  @DeepField(validationClass = classOf[SetType[_]])
  var emails: java.util.Set[String] = _

  @BeanProperty
  @DeepField(validationClass = classOf[ListType[_]])
  var phones: java.util.List[String] = _

  @BeanProperty
  @DeepField(validationClass = classOf[MapType[_, _]])
  var uuid2id: java.util.Map[UUID, Integer] = _

}
