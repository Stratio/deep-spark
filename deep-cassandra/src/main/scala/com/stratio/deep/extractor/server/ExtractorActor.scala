package com.stratio.deep.extractor.server

import java.lang.reflect.Constructor

import akka.actor.Actor
import com.stratio.deep.config.ExtractorConfig
import com.stratio.deep.rdd.{DeepTokenRange, CassandraExtractor, IExtractor}
import scala.reflect.ClassTag

/**
 * Created by darroyo on 22/08/14.
 */
class ExtractorActor[T: ClassTag] extends Actor{ //with ActionSystemMessage{

  protected var extractorA: CassandraExtractor[T] = null

  //private var extractor1: IExtractor = null

  def receive = {


    case CloseAction() =>
      println("message received")
      extractorA.close()
      sender ! new CloseResponse(true)


    case GetPartitionsAction(config) =>
      println("message received")

      if (extractorA == null) {
        val rdd: Class[T] = config.getRDDClass.asInstanceOf[Class[T]]
        try {
          extractorA = rdd.newInstance().asInstanceOf[CassandraExtractor[T]]

        }
        catch {
          case e: Any => {
            e.printStackTrace
          }
        }

      }
      sender ! new GetPartitionsResponse(extractorA.getPartitions(config.asInstanceOf[ExtractorConfig[T]]));


    case  HasNextAction() =>
      println("message received")
      sender ! new HasNextResponse(extractorA.hasNext)



    case InitIteratorAction(partition,config) =>
      println("message received")
      if (extractorA == null) {
        val rdd: Class[T] = config.getRDDClass.asInstanceOf[Class[T]]
        try {
          extractorA = rdd.newInstance().asInstanceOf[CassandraExtractor[T]]

        }
        catch {
          case e: Any => {
            e.printStackTrace
          }
        }

      }
      extractorA.initIterator(partition, config.asInstanceOf[ExtractorConfig[T]])

      sender ! new InitIteratorResponse(true);

    case NextAction() =>
     sender ! new NextResponse(extractorA.next().asInstanceOf[T])
     // this.next()
     // sender() ! DivisionResult(n1, n2, n1 / n2)
  }



}
