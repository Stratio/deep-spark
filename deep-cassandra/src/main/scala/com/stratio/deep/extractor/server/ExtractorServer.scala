package com.stratio.deep.extractor.server

import java.lang.reflect.Constructor

import akka.actor.Actor
import com.stratio.deep.config.ExtractorConfig
import com.stratio.deep.extractor.message._
import com.stratio.deep.extractor.response._
import com.stratio.deep.rdd.{DeepTokenRange, CassandraExtractor, IExtractor}
import scala.reflect.ClassTag

/**
 * Created by darroyo on 22/08/14.
 */
class ExtractorServer[T: ClassTag] extends Actor{ //with ActionSystemMessage{

  protected var extractor: CassandraExtractor[T] = null

  //private var extractor1: IExtractor = null

  def receive = {

    case CloseAction() =>
    //  sender() ! AddResult(n1, n2, n1 + n2)
    case GetPartitionsAction(config) =>
    //  val partitionsAction: GetPartitionsAction[T] = action.asInstanceOf[GetPartitionsAction[T]]
      //response = new GetPartitionsResponse(this.getPartitions(partitionsAction))
    //  sender() ! SubtractResult(n1, n2, n1 - n2)
    case  HasNextAction() =>
    //  sender() ! MultiplicationResult(n1, n2, n1 * n2)
    case InitIteratorAction(partition , config[T]) =>


      if (extractor == null) {

        val rdd: Class[T] = config.getRDDClass.asInstanceOf[Class[T]]
        try {
          val extractor = rdd.newInstance().asInstanceOf[CassandraExtractor[T]]
          //extractor = c.newInstance.asInstanceOf[CassandraExtractor[T]]
        }
        catch {
          case e: Any => {
            e.printStackTrace
          }
        }

      }
      extractor.initIterator(partition, config)




      sender() ! new InitIteratorResponse()
    case NextAction() =>
     // this.next()
     // sender() ! DivisionResult(n1, n2, n1 / n2)
  }

  protected def next(nextAction: NextAction[T]): T = {
    return extractor.next
  }

  protected def close {
    extractor.close
    return
  }

  /*protected def initIterator(partition: DeepTokenRange, config: ExtractorConfig[T]) {
    if (extractor == null) {
      this.initExtractor(partition)
    }
    extractor.initIterator(partition, config)
    return
  }

*/
  /**
   * @param config
   */
  /*@SuppressWarnings(Array("unchecked"))
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

*/

}
