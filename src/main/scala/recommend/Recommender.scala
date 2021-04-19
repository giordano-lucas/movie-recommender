package recommend

import customFiles._
import org.apache.spark.rdd.RDD

class Conf(arguments: Seq[String]) extends JsonConf(arguments) {
  val data = opt[String](required = true)
  val personal = opt[String](required = true)
  verify()
}

object Recommender extends App {
  // Remove these lines if encountering/debugging Spark
  implicit val spark = Setup.initialise
  implicit val conf  = new Conf(args) 

  println("")
  println("******************************************************")
  println("Loading data from: " + conf.data())
  val dataFile = spark.sparkContext.textFile(conf.data())
  val data     = Setup.readFile(dataFile)

  assert(data.count == 100000, "Invalid data")

  println("Loading personal data from: " + conf.personal())
  val personalFile = spark.sparkContext.textFile(conf.personal())
  // TODO: Extract ratings and movie titles
  assert(personalFile.count == 1682, "Invalid personal data")

  // **********************************************************************************************
  // ********************************* Movie Class Definition *************************************
  // **********************************************************************************************

  // define movie case class
  case class Movie(item: Int, name: String, rating: Option[Double])
  // define ordering for movies : first rating then - id
  implicit val ord:Ordering[Movie] = Ordering.by(m => (m.rating getOrElse -1.0, - m.item))

  // **********************************************************************************************
  // ********************************** Personal File reading *************************************
  // **********************************************************************************************

  val persoId = 944 // my own user id
  val movies   = personalFile.map(l => {                    // create RDD of Movies from personalFile
      val cols = l.split(",").map(_.trim)                   // split on ',' but note that I had to change this row: {177,"Good The Bad and the Ugly The"} to make it work
      Movie(                                                // create a movie
        cols(0).toInt, 
        cols(1),
        if (cols.size < 3) None else Some(cols(2).toDouble) // encode the possibily of non personal rating using Options  
      )
    })     
  val perso = movies.filter(_.rating.isDefined)                       // only consider rows where there is a rating
                    .map(m => Rating(persoId, m.item, m.rating.get )) // convert into rating
  
  val unionData = data union perso // union with data file => new training set

  // **********************************************************************************************
  // ******************************* Recommendation computation ***********************************
  // **********************************************************************************************

  val h              = Helper(unionData)  // create helper as usual                                                    
  val persoRu:Double = unionData
                        .filter(_.user == persoId)
                        .map(_.rating)
                        .mean() // only consider personal recommandations
  val items          = h.prep_ratings
                        .filter(_.user == persoId)
                        .map(r => (r.item,r.rating))
                        .collectAsMap
  val cosSim = h.prep_ratings
      .filter(items isDefinedAt _.item)
      .map {case Rating(v,i,r) => (v, r * items(i))}
      .reduceByKey(_ + _)
      .sortBy(-_._2) // descending sort on similarities
      .take(300)  
  val sim300 = cosSim         .toMap.withDefaultValue(0.0) // map default value of 0.0
  val sim30  = cosSim.take(30).toMap.withDefaultValue(0.0) // map default value of 0.0
  // prediction
  def pred(mapSim:Map[Int,Double]) = {
    val dev = h.scaled_train
                  .map{ case Rating(v,i,r) => (i,(r * mapSim(v), mapSim(v).abs))}
                  .reduceByKey {case ((a1,b1),(a2,b2)) => (a1+a2,b1+b2)}
                  .mapValues{case (num,den) => if (den == 0.0) 0.0 else num/den}
    dev.mapValues(Helper.baselinePrediction(persoRu,_))          // perform baseline prediction
  }
  def recommendations(preds:RDD[(Int,Double)]) = movies.filter(!_.rating.isDefined) // do not want to predict an already rated movie
                              .map(m => (m.item,m))                                 // prepare for join
                              .leftOuterJoin(preds).values                          // join with predictions
                              .map {case (m,pred) => Movie(m.item,m.name, Some(pred getOrElse h.globalAvgRating))} // create new Movie with predicted rating
                              .top(5)                                               // take top 5 recommandations
  // format a list of movies into the output required in the JSON file.
  def formatRecommendations(ls:Seq[Movie]) = ls.map(m => List(m.item,m.name,m.rating))
  // **********************************************************************************************
  // **********************************************************************************************
  // **********************************************************************************************
  // Save answers as JSON
  Setup.outputAnswers(Map(
          // IMPORTANT: To break ties and ensure reproducibility of results,
          // please report the top-5 recommendations that have the smallest
          // movie identifier.
          "Q3.2.5" -> Map(
            "Top5WithK=30"  -> formatRecommendations(recommendations(pred(sim30))),
            "Top5WithK=300" -> formatRecommendations(recommendations(pred(sim300)))
            // Discuss the differences in rating depending on value of k in the report.
          )
        )
      )
  Setup.terminate
}
