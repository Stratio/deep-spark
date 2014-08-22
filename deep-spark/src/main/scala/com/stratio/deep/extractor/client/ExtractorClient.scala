package com.stratio.deep.extractor.client

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import scala.reflect.ClassTag

import com.stratio.deep.config.ExtractorConfig
import com.stratio.deep.extractor.message.CloseAction
import com.stratio.deep.extractor.message.GetPartitionsAction
import com.stratio.deep.extractor.message.HasNextAction
import com.stratio.deep.extractor.message.InitIteratorAction
import com.stratio.deep.extractor.message.NextAction
import com.stratio.deep.rdd.DeepTokenRange

import akka.actor.ActorSystem
import akka.pattern.ask
import akka.util.Timeout

class ExtractorClient[T: ClassTag](inetAddr: String) {

  implicit val system = ActorSystem()
  implicit val timeout = Timeout(5 seconds)

  val extractor = system.actorSelection("akka.tcp://ExtractorWorkerSystem@" + inetAddr + "/user/extractor")

  def getPartitions(config: ExtractorConfig[T]): Array[DeepTokenRange] = {
    val response = extractor ? GetPartitionsAction(config)
    Await.result(response, timeout.duration).asInstanceOf[Array[DeepTokenRange]]
  }

  def hasNext(): Boolean ={
    val response = extractor ? HasNextAction()
    Await.result(response, timeout.duration).asInstanceOf[Boolean]
  }

  def next(): T = {
    val response = extractor ? NextAction()
    Await.result(response, timeout.duration).asInstanceOf[T]
  }

  def close(): Boolean = {
    val response = extractor ? CloseAction()
    Await.result(response, timeout.duration).asInstanceOf[Boolean]
  }

  def initIterator(dp: DeepTokenRange, config: ExtractorConfig[T]): Boolean = {
    val response = extractor ? InitIteratorAction(dp, config)
    Await.result(response, timeout.duration).asInstanceOf[Boolean]
  }
}