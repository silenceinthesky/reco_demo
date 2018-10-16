package org.example.recommendation

import org.apache.predictionio.controller.PDataSource
import org.apache.predictionio.controller.EmptyEvaluationInfo
import org.apache.predictionio.controller.Params
import org.apache.predictionio.data.storage.Event
import org.apache.predictionio.data.store.PEventStore

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD


import grizzled.slf4j.Logger

case class DataSourceEvalParams(kFold: Int, queryNum: Int)

case class DataSourceParams(
  appName: String,
  evalParams: Option[DataSourceEvalParams]) extends Params

class DataSource(val dsp: DataSourceParams)
  extends PDataSource[TrainingData,
      EmptyEvaluationInfo, Query, ActualResult] {

  @transient lazy val logger = Logger[this.type]

  def getRatings(sc: SparkContext): RDD[Rating] = {

    val eventsRDD: RDD[Event] = PEventStore.find(
      appName = dsp.appName,
      entityType = Some("user"),
      eventNames = Some(List("consume")), // read "rate" and "buy" event
      // targetEntityType is optional field of an event.
      targetEntityType = Some(Some("item")))(sc)

    val ratingsRDD: RDD[Rating] = eventsRDD.map { event =>
      val rating = try {
        val ratingValue: Double = event.event match {
          case "consume" => event.properties.get[String]("consume_cnt").toDouble
          //case "buy" => 4.0 // map buy event to rating value of 4
          case _ => throw new Exception(s"Unexpected event ${event} is read.")
        }
        logger.info(s"bournexiao, after PEventStore find.ratingValue:$ratingValue")
        // entityId and targetEntityId is String
        Rating(event.entityId,
          event.targetEntityId.get,
          ratingValue)
      } catch {
        case e: Exception => {
          logger.error(s"Cannot convert ${event} to Rating. Exception: ${e}.")
          throw e
        }
      }
      rating
    }.cache()

    ratingsRDD
  }

  override
  def readTraining(sc: SparkContext): TrainingData = {
    // create a RDD of (entityID, User)
    val usersRDD: RDD[(String, User)] = PEventStore.aggregateProperties(
      appName = dsp.appName,
      entityType = "user"
    )(sc).map { case (entityId, properties) =>
      val user = try {
        User()
      } catch {
        case e: Exception => {
          logger.error(s"111 Failed to get properties ${properties} of" +
            s" user ${entityId}. Exception: ${e}.")
          throw e
        }
      }
      (entityId, user)
    }.cache()

    // create a RDD of (entityID, Item)
    val itemsRDD: RDD[(String, Item)] = PEventStore.aggregateProperties(
      appName = dsp.appName,
      entityType = "item"
    )(sc).map { case (entityId, properties) =>
      val item = try {
        // Assume categories is optional property of item.
        // MODIFIED
        Item(
          item_name = properties.get[String]("item_name"),
          categories = properties.get[String]("category"))
      } catch {
        case e: Exception => {
          logger.error(s"222 Failed to get properties ${properties} of" +
            s" item ${entityId}. Exception: ${e}.")
          throw e
        }
      }
      (entityId, item)
    }.cache()


    new TrainingData(ratings=getRatings(sc), users=usersRDD, items=itemsRDD)

  }

//  override
//  def readEval(sc: SparkContext)
//  : Seq[(TrainingData, EmptyEvaluationInfo, RDD[(Query, ActualResult)])] = {
//    require(!dsp.evalParams.isEmpty, "Must specify evalParams")
//    val evalParams = dsp.evalParams.get
//
//    val kFold = evalParams.kFold
//    val ratings: RDD[(Rating, Long)] = getRatings(sc).zipWithUniqueId
//    ratings.cache
//
//    (0 until kFold).map { idx => {
//      val trainingRatings = ratings.filter(_._2 % kFold != idx).map(_._1)
//      val testingRatings = ratings.filter(_._2 % kFold == idx).map(_._1)
//
//      val testingUsers: RDD[(String, Iterable[Rating])] = testingRatings.groupBy(_.user)
//
//      (new TrainingData(trainingRatings),
//        new EmptyEvaluationInfo(),
//        testingUsers.map {
//          case (user, ratings) => (Query(user, evalParams.queryNum), ActualResult(ratings.toArray))
//        }
//      )
//    }}
//  }
}

case class User()

case class Item(
               item_name:String,
               categories: String
               )

case class Rating(
  user: String,
  item: String,
  rating: Double
)

class TrainingData(
  val ratings: RDD[Rating],
  val users: RDD[(String, User)],
  val items:RDD[(String, Item)]
                  ) extends Serializable {
  override def toString = {
    s"ratings: [${ratings.count()}] (${ratings.take(2).toList}...)"
  }
}

