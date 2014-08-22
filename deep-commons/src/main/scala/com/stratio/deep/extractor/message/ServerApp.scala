package com.stratio.deep.extractor.message

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory

/**
 * Created by darroyo on 22/08/14.
 */
object ServerApp{// extends App{

def main(args: Array[String]) {
  ActorSystem("ExtractorWorkerSystem", ConfigFactory.load("extractor"))
  println("Started ExtractorWorkerSystem")
}

}
