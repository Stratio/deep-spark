package com.stratio.deep.extractor.client

import akka.actor._
import akka.actor.ActorDSL._
import com.stratio.deep.rdd.DeepTokenRange
import com.stratio.deep.config.ExtractorConfig
import scala.reflect.ClassTag
import com.stratio.deep.extractor.message.ResponseSystemMessage
import com.stratio.deep.extractor.message.CloseResponse

class ExtractorClientAkka(remotePath: String) extends Actor {

  implicit val system = ActorSystem("ExtractionSystem-1")

  val extractor = system.actorSelection(remotePath + "/user/extractor")

  def receive = {
    case msg: ResponseSystemMessage => msg match {
      case CloseResponse(isClosed: Boolean) =>
        println("joe received '" + msg + "' from " + sender)
      case _ =>
        println("Other")
    }
    case _ => println("Received unknown msg ")
  }

  def getPartitions[T: ClassTag](config: ExtractorConfig[T]): Array[DeepTokenRange] = null

  def hasNext(): Boolean =
    true

  //  def next[T: ClassTag](): T = ClassTag.asInstanceOf

  def close() = {}

  def initIterator[T: ClassTag](dp: DeepTokenRange, config: ExtractorConfig[T]) = {}
}