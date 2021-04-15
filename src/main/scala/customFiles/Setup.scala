package customFiles

import org.rogach.scallop._
import org.json4s.jackson.Serialization
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

// move Rating def in this file to factorise code
case class Rating(user: Int, item: Int, rating: Double) 

// create JsonConf with default 'json' field to be able to write the : 'outputAnswers' using implicits in this file.
class JsonConf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val json = opt[String]()
}

// main object of the file
object Setup {
  // set up spark
  def initialise:SparkSession = {
    // Remove these lines if encountering/debugging Spark
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val spark = SparkSession.builder()
      .master("local[1]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR") 
    println("")
    println("******************************************************")
    spark // return spark object
  }
  // kills the spark session
  def terminate(implicit spark:SparkSession):Unit = {
    println("")
    spark.close()
  }
  // read data file and convert into RDD of ratings
  def readFile(dataFile:RDD[String]):RDD[Rating] = 
    dataFile.map(l => {
      val cols = l.split("\t").map(_.trim)
      Rating(cols(0).toInt, cols(1).toInt, cols(2).toDouble)
    }) 
  // Save answers as JSON
  def printToFile(content: String, 
                  location: String = "./answers.json") =
    Some(new java.io.PrintWriter(location)).foreach{
      f => try{
        f.write(content)
      } finally{ f.close }
  }
  // output Answers
  def outputAnswers(answers: Map[String, Any])(implicit conf:JsonConf) = 
    conf.json.toOption match {
    case None => ; 
    case Some(jsonFile) => {
      var json = "";
      {
        // Limiting the scope of implicit formats with {}
        implicit val formats = org.json4s.DefaultFormats
        json = Serialization.writePretty(answers)
      }
      println(json)
      println("Saving answers in: " + jsonFile)
      printToFile(json, jsonFile)
    }
  }
}