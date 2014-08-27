package com.stratio.deep.extractor.client

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import scala.reflect.ClassTag
import scala.reflect.ClassTag$
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
import akka.actor.Props
import com.stratio.deep.extractor.message.NextResponse
import com.stratio.deep.extractor.message.HasNextResponse
import com.stratio.deep.extractor.message.GetPartitionsResponse
import com.stratio.deep.extractor.message.CloseResponse
import com.stratio.deep.extractor.message.InitIteratorResponse

class ExtractorClient[T: ClassTag](inetAddr: String) {

  implicit val system = ActorSystem()
  implicit val timeout = Timeout(5 seconds)

  val extractor = system.actorOf(Props(classOf[ExtractorActor[T]], ClassTag$.MODULE$.Any), "extractor")
  //  val extractor = system.actorSelection("akka.tcp://ExtractorWorkerSystem@" + inetAddr + "/user/extractor")

  def getPartitions(config: ExtractorConfig[T]): Array[DeepTokenRange] = {
    val response = extractor ? GetPartitionsAction(config)
    val result = Await.result(response, timeout.duration).asInstanceOf[GetPartitionsResponse]
    result.partitions
  }

  def hasNext(): Boolean ={
    val response = extractor ? HasNextAction()
    val result = Await.result(response, timeout.duration).asInstanceOf[HasNextResponse]
    result.hasNext
  }

  def next(): T = {
    val response = extractor ? NextAction()
    val result = Await.result(response, timeout.duration).asInstanceOf[NextResponse[T]]
    result.data
  }

  def close(): Boolean = {
    val response = extractor ? CloseAction()
    val result = Await.result(response, timeout.duration).asInstanceOf[CloseResponse]
    result.isClosed
  }

  def initIterator(dp: DeepTokenRange, config: ExtractorConfig[T]): Boolean = {
    val response = extractor ? InitIteratorAction(dp, config)
    val result = Await.result(response, timeout.duration).asInstanceOf[InitIteratorResponse]
    result.isInitialized
  }
}