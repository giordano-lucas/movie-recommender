
package customFiles
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

// helper class
sealed class Helper(data:RDD[Rating]) {
  // variables first defined in Analyser
   val globalPred = 3.0
   val globalAvgRating = data.map(_.rating).mean() // average rating computation
   val user_ratings = data.groupBy(_.user)                   // groupby userId
                         .mapValues(Helper.meanRatings(_))   // mean of ratings
   val item_ratings = data.groupBy(_.item)                   // groupby itemId
                         .mapValues(Helper.meanRatings(_))   // mean of ratings
}
// define companion object of Helper. Note that we are required to put the functions 'scale' and 'baselinePrediction'
// here to make them recognizable as serialisable for Spark.
object Helper {
  // define apply to avoid having to use the 'new' keyword
  def apply(data:RDD[Rating]) = new Helper(data)
  // construct baseline prediction
  def scale(x:Double,ru:Double) =
    if      (x > ru)  5-ru
    else if (x < ru)  ru-1
    else              1
  // variables first defined in Predictor
  def baselinePrediction(ru:Double,ri:Double)   = ru + ri * scale(ru+ri,ru)
  // mean function for lists of Ratings
  def meanRatings[T <: Iterable[Rating]] (rs:T) = rs.map(_.rating).sum / rs.size.toDouble
}

