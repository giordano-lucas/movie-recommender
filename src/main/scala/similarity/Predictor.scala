package similarity

import org.rogach.scallop._
import org.json4s.jackson.Serialization
import org.apache.spark.rdd.RDD

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val train = opt[String](required = true)
  val test = opt[String](required = true)
  val json = opt[String]()
  verify()
}

case class Rating(user: Int, item: Int, rating: Double)

object Predictor extends App {
  // Remove these lines if encountering/debugging Spark
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  val spark = SparkSession.builder()
    .master("local[1]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  println("")
  println("******************************************************")

  var conf = new Conf(args)
  println("Loading training data from: " + conf.train())
  val trainFile = spark.sparkContext.textFile(conf.train())
  val train = trainFile.map(l => {
      val cols = l.split("\t").map(_.trim)
      Rating(cols(0).toInt, cols(1).toInt, cols(2).toDouble)
  })
  assert(train.count == 80000, "Invalid training data")

  println("Loading test data from: " + conf.test())
  val testFile = spark.sparkContext.textFile(conf.test())
  val test = testFile.map(l => {
      val cols = l.split("\t").map(_.trim)
      Rating(cols(0).toInt, cols(1).toInt, cols(2).toDouble)
  })
  assert(test.count == 20000, "Invalid test data")

  // Save answers as JSON
  def printToFile(content: String,
                  location: String = "./answers.json") =
    Some(new java.io.PrintWriter(location)).foreach{
      f => try{
        f.write(content)
      } finally{ f.close }
  }
  conf.json.toOption match {
    case None => ;
    case Some(jsonFile) => {
      var json = "";
      {
        // Limiting the scope of implicit formats with {}
        implicit val formats = org.json4s.DefaultFormats
        val answers: Map[String, Any] = Map(
          "Q2.3.1" -> Map(
            "CosineBasedMae" -> 0.0, // Datatype of answer: Double
            "PredictionAccuracyImproved" -> false // Datatype of answer: Bool
          ),

          "Q2.3.2" -> Map(
            "AvailableSimilarityTechniques" -> List("Jaccard", "Sørensen–Dice", "Szymkiewicz–Simpson"), // Do not modify this line!
            "SelectedSimilarityTechnique" -> "Jaccard", // Datatype of answer: String. Pick an element from `AvailableSimilarityTechniques`
            "SelectedSimilarityTechniqueMae" -> 0.0, // Datatype of answer: Double
            "SelectedBetterThanCosine" -> false // Datatype of answer: Bool
          ),

          "Q2.3.3" -> Map(
            "NumberOfSimilarityComputationsAsFunctionOfU" -> "Provide formula on report", // Do not modify this line!
            "NumberOfSimilarityComputationsForWorkingDataset" -> 0 // Datatype of answer: Int
          ),

          "Q2.3.4" -> Map(
            "MinimumNumberOfMultiplications" -> 0, // Datatype of answer: Int
            "CosineSimilarityStatistics" -> Map(
              "min" -> 0.0,  // Datatype of answer: Double
              "max" -> 0.0, // Datatype of answer: Double
              "average" -> 0.0, // Datatype of answer: Double
              "stddev" -> 0.0 // Datatype of answer: Double
            )
          ),

          "Q2.3.5" -> Map(
            "MemoryRequiredToStoreSimilarityComputationsAsFunctionOfU" -> "Provide formula on report", // Do not modify this line!
            "TotalBytesToStoreNonZeroSimilarityComputationsForWorkingDataset" -> 0 // Datatype of answer: Int
          ),

          "Q2.3.6" -> Map(
            "DurationInMicrosecForComputingPredictions" -> Map(
              "min" -> 0.0,  // Datatype of answer: Double
              "max" -> 0.0, // Datatype of answer: Double
              "average" -> 0.0, // Datatype of answer: Double
              "stddev" -> 0.0 // Datatype of answer: Double
            ),
            "SimilarityBasedMethodDurationComparisonWithMethodsOfMilestone1" -> "Provide answer on report" // Do not modify this line!
          ),

          "Q2.3.7" -> Map(
            "DurationInMicrosecForComputingSimilarities" -> Map(
              "min" -> 0.0,  // Datatype of answer: Double
              "max" -> 0.0, // Datatype of answer: Double
              "average" -> 0.0, // Datatype of answer: Double
              "stddev" -> 0.0 // Datatype of answer: Double
            ),
            "AverageTimeInMicrosecPerSuv" -> 0, // Datatype of answer: Int
            "RatioBetweenTimeToComputeSimilarityOverTimeToPredict" -> 0.0 // Datatype of answer: Int
          )
         )
        json = Serialization.writePretty(answers)
      }

      println(json)
      println("Saving answers in: " + jsonFile)
      printToFile(json, jsonFile)
    }
  }

  println("")
  spark.close()
}
