/*
package com.stratio.deep.extractor.message

import com.stratio.deep.config.ExtractorConfig
import com.stratio.deep.extractor.response._
import com.stratio.deep.rdd.{IExtractor, DeepTokenRange}
import akka.actor.Actor
import scala.reflect.ClassTag


/**
 * Created by darroyo on 22/08/14.
 */
class ExtractorServer extends Actor{ //with ActionSystemMessage{

  private var extractor: IExtractor = null

  def receive = {

    case CloseAction() =>
      sender() ! AddResult(n1, n2, n1 + n2)
    case GetPartitionsAction(config) =>
    //  val partitionsAction: GetPartitionsAction[T] = action.asInstanceOf[GetPartitionsAction[T]]
      response = new GetPartitionsResponse(this.getPartitions(partitionsAction))
      sender() ! SubtractResult(n1, n2, n1 - n2)
    case  HasNextAction() =>
      sender() ! MultiplicationResult(n1, n2, n1 * n2)
    case InitIteratorAction(partition , config) =>
      sender() ! DivisionResult(n1, n2, n1 / n2)
    case NextAction() =>
      sender() ! DivisionResult(n1, n2, n1 / n2)
  }

  protected def next(nextAction: NextAction[T]): T = {
    return extractor.next
  }

  protected def close {
    extractor.close
    return
  }

  protected def initIterator(initIteratorAction: InitIteratorAction[T]) {
    if (extractor == null) {
      this.initExtractor(initIteratorAction.getConfig)
    }
    extractor.initIterator(initIteratorAction.getPartition, initIteratorAction.getConfig)
    return
  }

  /**
   * @param config
   */
  @SuppressWarnings(Array("unchecked"))
  private def initExtractor(config: ExtractorConfig[T]) {
    val rdd: Class[T] = config.getRDDClass.asInstanceOf[Class[T]]
    try {
      val c: Constructor[T] = rdd.getConstructor
      this.extractor = c.newInstance.asInstanceOf[CassandraExtractor[T]]
    }
    catch {
      case e: Any => {
        e.printStackTrace
      }
    }
  }


   /* var response: Response = null
    action.getType match {
      case GET_PARTITIONS =>
        val partitionsAction: GetPartitionsAction[T] = action.asInstanceOf[GetPartitionsAction[T]]
        response = new GetPartitionsResponse(this.getPartitions(partitionsAction))
        break //todo: break is not supported
      case CLOSE =>
        this.close
        response = new CloseResponse
        break //todo: break is not supported
      case HAS_NEXT =>
        val hasNextAction: HasNextAction[T] = action.asInstanceOf[HasNextAction[T]]
        response = new HasNextResponse(this.hastNext(hasNextAction))
        break //todo: break is not supported
      case NEXT =>
        val nextAction: NextAction[T] = action.asInstanceOf[NextAction[T]]
        response = new NextResponse[T](this.next(nextAction))
        break //todo: break is not supported
      case INIT_ITERATOR =>
        val initIteratorAction: InitIteratorAction[T] = action.asInstanceOf[InitIteratorAction[T]]
        this.initIterator(initIteratorAction)
        response = new InitIteratorResponse[_]
        break //todo: break is not supported
      case _ =>
        break //todo: break is not supported
    }
    ctx.write(response)
  }

  override def channelReadComplete(ctx: ChannelHandlerContext) {
    ctx.flush
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
    cause.printStackTrace
    ctx.close
  }

  protected def hastNext(hasNextAction: HasNextAction[_]): Boolean = {
    return extractor.hasNext
  }





  protected def getPartitions(getPartitionsAction: GetPartitionsAction[T]): Array[DeepTokenRange] = {
    if (extractor == null) {
      this.initExtractor(getPartitionsAction.getConfig)
    }
    return extractor.getPartitions(getPartitionsAction.getConfig)
  }

  /**
   * @param config
   */
  @SuppressWarnings(Array("unchecked")) private def initExtractor(config: ExtractorConfig[T]) {
    val rdd: Class[T] = config.getRDDClass.asInstanceOf[Class[T]]
    try {
      val c: Constructor[T] = rdd.getConstructor
      this.extractor = c.newInstance.asInstanceOf[CassandraExtractor[T]]
    }
    catch {
      case e: Any => {
        e.printStackTrace
      }
    }
  }*/

}
*/
