package org.example.recommendation

import org.apache.predictionio.controller.PPreparator

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class Preparator
  extends PPreparator[TrainingData, PreparedData] {

  def prepare(sc: SparkContext, trainingData: TrainingData): PreparedData = {
    new PreparedData(ratings = trainingData.ratings, users=trainingData.users, items=trainingData.items)
  }
}

class PreparedData(
  val ratings: RDD[Rating],
  val users: RDD[(String, User)],
  val items:RDD[(String, Item)]
) extends Serializable


