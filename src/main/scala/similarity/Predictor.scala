package similarity

import customFiles._
import org.apache.spark.rdd.RDD
import scala.collection.immutable.TreeSet

class Conf(arguments: Seq[String]) extends JsonConf(arguments) {
  val train = opt[String](required = true)
  val test = opt[String](required = true)
  verify()
}

object Predictor extends App {
  // Remove these lines if encountering/debugging Spark
  implicit val spark = Setup.initialise
  implicit val conf  = new Conf(args) 

  println("")
  println("******************************************************")
  println("Loading training data from: " + conf.train())
  val trainFile = spark.sparkContext.textFile(conf.train())
  val train     = Setup.readFile(trainFile)
  assert(train.count == 80000, "Invalid training data")

  println("Loading test data from: " + conf.test())
  val testFile = spark.sparkContext.textFile(conf.test())
  val test     = Setup.readFile(testFile)

  // **********************************************************************************************
  // **********************************************************************************************
  // **********************************************************************************************

  //val sqlContext = new org.apache.spark.sql.SQLContext(spark.sparkContext)
  // this is used to implicitly convert an RDD to a DataFrame.
  //import sqlContext.implicits._
  //val df = train.toDF()
  //************************************************
  val h = Helper(train)


  // 2.1 Preprocessing Ratings
  
  

  def cosinePred = test.map(r => (r.user,r))                         // format pair for join on users
                        .join(h.user_ratings)                   // first join on ratings
                        .map {case (u,(r,ru)) => (r.item, (u,ru, r.rating))} // format pair for join on items
                        .leftOuterJoin(h.avgItemDev).values            // perform left join to catch items without rating in train set.
                        .map {case ((u,ru,r),opt_ri) =>                  // compute baseline prediction    
                          (r, (opt_ri map { mapDev => Helper.baselinePrediction(ru,mapDev(u))} getOrElse h.globalAvgRating))
                        } // Note: output gloabal average if no item rating is available  
  // report mae
  def mae(rdd: RDD[(Double,Double)]) =  // mean average error function
    rdd.map{ case (rat, pred) => (rat - pred).abs}.mean()
  // Question 2.3.1.
  val cosine_mae = mae(cosinePred)
  // **********************************************************************************************
  // **********************************************************************************************
  // **********************************************************************************************

  Setup.outputAnswers(Map(
          "Q2.3.1" -> Map(
            "CosineBasedMae" -> cosine_mae, // Datatype of answer: Double
            "CosineMinusBaselineDifference" -> (cosine_mae-0.7669)// Datatype of answer: Double
          ),

          "Q2.3.2" -> Map(
            "JaccardMae" -> 0.0, // Datatype of answer: Double
            "JaccardMinusCosineDifference" -> 0.0 // Datatype of answer: Double
          ),

          "Q2.3.3" -> Map(
            // Provide the formula that computes the number of similarity computations
            // as a function of U in the report.
            "NumberOfSimilarityComputationsForU1BaseDataset" -> 0 // Datatype of answer: Int
          ),

          "Q2.3.4" -> Map(
            "CosineSimilarityStatistics" -> Map(
              "min" -> 0.0,  // Datatype of answer: Double
              "max" -> 0.0, // Datatype of answer: Double
              "average" -> 0.0, // Datatype of answer: Double
              "stddev" -> 0.0 // Datatype of answer: Double
            )
          ),

          "Q2.3.5" -> Map(
            // Provide the formula that computes the amount of memory for storing all S(u,v)
            // as a function of U in the report.
            "TotalBytesToStoreNonZeroSimilarityComputationsForU1BaseDataset" -> 0 // Datatype of answer: Int
          ),

          "Q2.3.6" -> Map(
            "DurationInMicrosecForComputingPredictions" -> Map(
              "min" -> 0.0,  // Datatype of answer: Double
              "max" -> 0.0, // Datatype of answer: Double
              "average" -> 0.0, // Datatype of answer: Double
              "stddev" -> 0.0 // Datatype of answer: Double
            )
            // Discuss about the time difference between the similarity method and the methods
            // from milestone 1 in the report.
          ),

          "Q2.3.7" -> Map(
            "DurationInMicrosecForComputingSimilarities" -> Map(
              "min" -> 0.0,  // Datatype of answer: Double
              "max" -> 0.0, // Datatype of answer: Double
              "average" -> 0.0, // Datatype of answer: Double
              "stddev" -> 0.0 // Datatype of answer: Double
            ),
            "AverageTimeInMicrosecPerSuv" -> 0.0, // Datatype of answer: Double
            "RatioBetweenTimeToComputeSimilarityOverTimeToPredict" -> 0.0 // Datatype of answer: Double
          )
         )
    )
  Setup.terminate
}
