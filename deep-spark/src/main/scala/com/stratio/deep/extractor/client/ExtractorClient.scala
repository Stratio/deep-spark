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
import akka.actor.Deploy
import akka.remote.RemoteScope
import akka.actor.Address
import com.typesafe.config.ConfigFactory
import com.stratio.deep.extractor.server.ExtractorActor

class ExtractorClient[T: ClassTag](host: String, port: Integer) {

  implicit val system = ActorSystem("ExtractorWorkerSystem-1", ConfigFactory.load("extractor"))
  implicit val timeout = Timeout(600 seconds)

  val inetAddr = Address("akka.tcp", "ExtractorWorkerSystem", host, port)
  val extractor = system.actorOf(Props(classOf[ExtractorActor[T]], ClassTag$.MODULE$.Any).withDeploy(Deploy(scope = RemoteScope(inetAddr))), name = "extractorActor")
//  val extractor = system.actorSelection("akka.tcp://ExtractorWorkerSystem@172.19.0.133:2552/user/extractorActor")
//  val extractor = system.actorOf(Props(classOf[ExtractorActor[T]], ClassTag$.MODULE$.Any), "extractorActor")
//  val extractor = system.actorFor("akka://HelloRemoteSystem@" + inetAddr + "/user/ExtractorActor")
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