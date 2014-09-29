package com.stratio.deep.extractor.message

import com.stratio.deep.commons.config.ExtractorConfig
import com.stratio.deep.commons.rdd.DeepTokenRange

import scala.reflect.ClassTag

// Messages the server can handle
sealed trait ActionSystemMessage

case class CloseAction() extends ActionSystemMessage

case class GetPartitionsAction[T: ClassTag](config: ExtractorConfig[T]) extends ActionSystemMessage

case class HasNextAction() extends ActionSystemMessage

case class InitIteratorAction[T: ClassTag](partition: DeepTokenRange, config: ExtractorConfig[T]) extends ActionSystemMessage

case class NextAction() extends ActionSystemMessage

// Messages the client can handle 
sealed trait ResponseSystemMessage

case class CloseResponse(isClosed: Boolean) extends ResponseSystemMessage

case class GetPartitionsResponse(partitions: Array[DeepTokenRange]) extends ResponseSystemMessage

case class HasNextResponse(hasNext: Boolean) extends ResponseSystemMessage

case class InitIteratorResponse(isInitialized: Boolean) extends ResponseSystemMessage

case class NextResponse[T: ClassTag](data: T) extends ResponseSystemMessage
