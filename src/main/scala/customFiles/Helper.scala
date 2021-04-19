
package customFiles
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

// helper class
sealed class Helper(train:RDD[Rating],test:RDD[Rating]) {
  // variables first defined in Analyser
   val globalPred = 3.0
   val globalAvgRating = train.map(_.rating).mean() // average rating computation
   val user_ratings    = train.groupBy(_.user)               // groupby userId
                         .mapValues(Helper.meanRatings(_))   // mean of ratings
   
  //****************************************************************************
  // 2.1 Preprocessing Ratings
  // scale ratings as done in milestone 1
  val scaled_train = 
    train.map(r => (r.user,r))
      .join(user_ratings).values
      .map {case (r,ru) => Rating(r.user,r.item, (r.rating - ru)/Helper.scale(r.rating,ru))}
  // preprocess ratings to incude the denominator of cos similarity
  val prep_ratings = 
    scaled_train.groupBy(_.user)  // groupby userId
                .flatMap {        // each Rating will be mapped to a new Rating with scaled rating 
                  case (_,rs) => rs.map(r => Rating(r.user,r.item, r.rating / Helper.l2_norm(rs)))
                  }               // mean of ratings
                .cache()          // cache since it is used several times
  //****************************************************************************
  // cosine similarity
  val cosSim = prep_ratings.map(r => (r.item,r))
              .groupByKey()
              .flatMap{case (_,us) =>
                for {
                  Rating(u,_,ru) <- us
                  Rating(v,_,rv) <- us
                  if (u != v)
                  } yield ((u,v), ru * rv)
              }.reduceByKey(_ + _)
              .map{case ((u,v),sim) => (u,(v,sim))}
              
  //****************************************************************************
}
// define companion object of Helper. Note that we are required to put the functions 'scale' and 'baselinePrediction'
// here to make them recognizable as serialisable for Spark.
object Helper {
  // define apply to avoid having to use the 'new' keyword
  def apply(train:RDD[Rating]) = new Helper(train,train)
  def apply(train:RDD[Rating],test:RDD[Rating]) = new Helper(train,test)
  // construct baseline prediction
  def scale(x:Double,ru:Double) =
    if      (x > ru)  5-ru
    else if (x < ru)  ru-1
    else              1
  // variables first defined in Predictor
  def baselinePrediction(ru:Double,ri:Double)   = ru + ri * scale(ru+ri,ru)
  // mean function for lists of Ratings
  def meanRatings[T <: Iterable[Rating]] (rs:T) = rs.map(_.rating).sum / rs.size.toDouble
  // l2 norm on ratings
  def l2_norm[T <: Iterable[Rating]] (rs:T) = math.sqrt(rs.map(r => r.rating * r.rating).sum)
  // mean average error function
  def mae(rdd: RDD[(Double,Double)]) = 
    rdd.map{ case (rat, pred) => (rat - pred).abs}.mean()

  
}

