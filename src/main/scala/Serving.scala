package org.example.recommendation

import org.apache.predictionio.controller.LServing

import grizzled.slf4j.Logger

class Serving
  extends LServing[Query, PredictedResult] {

  @transient lazy val logger = Logger[this.type]
  def serve(query: Query,
    predictedResults: Seq[PredictedResult]): PredictedResult = {
    logger.info(s"Bournexiao, Just for test:${query},  head:${predictedResults.head}")
    logger.info(s"type of Query:${query.getClass.getSimpleName}, type of PredictedResult:${predictedResults.getClass.getSimpleName}")
    predictedResults.head
  }
}
